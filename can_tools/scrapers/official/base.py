import uuid
from abc import ABC
from contextlib import closing
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd
import requests
from sqlalchemy.engine.base import Engine
from sqlalchemy.orm.session import sessionmaker

from can_tools.db_util import fast_append_to_sql
from can_tools.models import (
    TemptableOfficialHasLocation,
    TemptableOfficialNoLocation,
    build_insert_from_temp,
)
from can_tools.scrapers.base import DatasetBase


class StateDashboard(DatasetBase, ABC):
    """
    Definition of common parameters and values for scraping a State Dashboard

    Attributes
    ----------

    table_name: str = "covid_official"
        Name of database table to insert into
    pk: str = '("vintage", "dt", "location", "variable_id", "demographic_id")'
        Primary key on database table
    provider = "state"
        Provider here is state
    data_type: str = "covid"
        Data type is set to covid
    has_location: bool
        Must be set by subclasses. True if location code (fips code) appears in data
    state_fips: int
        Must be set by subclasses. The two digit state fips code (as an int)

    """

    table_name: str = "covid_official"
    pk: str = '("vintage", "dt", "location_id", "variable_id", "demographic_id")'
    provider = "state"
    data_type: str = "covid"
    has_location: bool
    state_fips: int
    location_type: str

    def _prep_df(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, str]:
        """
        prepare dataframe for `put` operation. Returns a modified DataFrame
        and the insert_op string
        """
        insert_op = str(uuid.uuid4())
        to_ins = df.rename(columns={"vintage": "last_updated"}).assign(
            insert_op=insert_op, provider=self.provider, state_fips=self.state_fips
        )
        if "location_type" not in list(to_ins):
            to_ins["location_type"] = self.location_type

        return to_ins, insert_op

    def _put_exec(self, engine: Engine, df: pd.DataFrame) -> None:
        "Internal _put method for dumping data using TempTable class"
        to_ins, insert_op = self._prep_df(df)
        print("Dataframe has {} rows to start".format(df.shape[0]))

        table = (
            TemptableOfficialHasLocation
            if self.has_location
            else TemptableOfficialNoLocation
        )

        worked = False
        with closing(sessionmaker(engine)()) as sess:
            try:
                fast_append_to_sql(to_ins, engine, table)
                print("Inserted all rows to temp table")

                # then insert from temp table
                ins = build_insert_from_temp(insert_op, table, engine)
                res = sess.execute(ins)
                sess.commit()
                print("Inserted {} rows".format(res.rowcount))
                worked = True
            finally:
                deleter = table.__table__.delete().where(table.insert_op == insert_op)
                res_delete = sess.execute(deleter)
                sess.commit()
                print("Removed the {} rows from temp table".format(res_delete.rowcount))

        return worked


class CountyDashboard(StateDashboard, ABC):
    """
    Parent class for scrapers working directly with County dashboards

    See `StateDashboard` for more information
    """

    provider: str = "county"


class FederalDashboard(StateDashboard, ABC):
    """
    Parent class for scrapers working directly with federal sources

    See `StateDashboard` for more information
    """

    provider: str = "federal"

    def _prep_df(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, str]:
        """
        prepare dataframe for `put` operation. Returns a modified DataFrame
        and the insert_op string
        """
        insert_op = str(uuid.uuid4())
        to_ins = df.rename(columns={"vintage": "last_updated"}).assign(
            insert_op=insert_op, provider=self.provider
        )
        if "location_type" not in list(to_ins):
            to_ins["location_type"] = self.location_type

        return to_ins, insert_op


class ArcGIS(StateDashboard, ABC):
    """
    Parent class for extracting data from an ArcGIS dashbaord

    Must define class variables:

    * `ARCGIS_ID`
    * `FIPS`

    in order to use this class
    """

    ARCGIS_ID: str

    def __init__(
        self,
        execution_dt: pd.Timestamp = pd.Timestamp.utcnow(),
        params: Optional[Dict[str, Union[int, str]]] = None,
    ):
        super().__init__(execution_dt)

        # Default parameter values
        if params is None:
            params: Dict[str, Union[int, str]] = {
                "f": "json",
                "where": "0=0",
                "outFields": "*",
                "returnGeometry": "false",
            }

        self.params = params

    def _esri_ts_to_dt(self, ts: int) -> pd.Timestamp:
        """Convert unix timestamp from ArcGIS to pandas Timestamp"""
        return pd.Timestamp.fromtimestamp(ts / 1000).normalize()

    def arcgis_query_url(self, service: str, sheet: Union[str, int], srvid: str) -> str:
        """
        Construct the arcgis query url given service, sheet, and srvid

        The correct value should be found by inspecting the network tab of the
        browser's developer tools

        Parameters
        ----------
        service : str
            The name of an argcis service
        sheet : Union[str,int]
            The sheet number containing the data of interest
        srvid : str
            The server id hosting the desired service

        Returns
        -------
        url: str
            The url pointing to the ArcGIS resource to be collected

        """
        out = f"https://services{srvid}.arcgis.com/{self.ARCGIS_ID}/"
        out += f"ArcGIS/rest/services/{service}/FeatureServer/{sheet}/query"

        return out

    def get_single_json(
        self, service: str, sheet: Union[str, int], srvid: str, params: Dict[str, Any]
    ) -> dict:
        """
        Execute request and return response json as dict
        Parameters
        ----------
        service, sheet, srvid :
            See `arcgis_query_url` method
        params : dict
            A dictionary of additional parameters to pass as the `params` argument
            to the `requests.get` method. These are turned into http query
            parameters by requests

        Returns
        -------
        js: dict
            A dict containing the JSON response from the making the HTTP request

        """
        # Perform actual request
        url = self.arcgis_query_url(service=service, sheet=sheet, srvid=srvid)
        res = requests.get(url, params=params)

        return res.json()

    def get_all_jsons(
        self, service: str, sheet: Union[str, int], srvid: str
    ) -> List[Dict]:
        """
        Repeatedly request jsons until we have full dataset

        Parameters
        ----------
        service, sheet, srvid :
            See `arcgis_query_url` method

        Returns
        -------
        the_jsons: list
            A dict containing the JSON response from the making the HTTP request
        """
        # Get a copy so that we don't screw up main parameters
        curr_params = self.params.copy()

        # Get first request and determine number of requests that come per
        # response
        res_json = self.get_single_json(service, sheet, srvid, curr_params)
        total_offset = len(res_json["features"])

        # Use first response to create first DataFrame
        the_jsons = [res_json]
        unbroken_chain = res_json.get("exceededTransferLimit", False)
        while unbroken_chain:
            # Update parameters and make request
            curr_params.update({"resultOffset": total_offset})
            res_json = self.get_single_json(service, sheet, srvid, curr_params)

            # Convert to DataFrame and store in df list
            the_jsons.append(res_json)

            total_offset += len(res_json["features"])
            unbroken_chain = res_json.get("exceededTransferLimit", False)

        return the_jsons

    def arcgis_json_to_df(self, res_json: dict) -> pd.DataFrame:
        """
        Parse the json returned from the main HTTP request into a DataFrame
        Parameters
        ----------
        res_json : dict
            Dict representation of JSON response from making HTTP call

        Returns
        -------
        df: pd.DataFrame
            A pandas DataFrame with all data from the attributes field of the
            `res_json["features"]` dict

        """
        df = pd.DataFrame.from_records([x["attributes"] for x in res_json["features"]])

        return df

    def arcgis_jsons_to_df(self, data: List[Dict]) -> pd.DataFrame:
        """
        Obtain all data in a particular ArcGIS service sheet as a DataFrame

        Parameters
        ----------
        data : List[Dict]
            A list of ArcGIS json objects

        Returns
        -------
        df: pd.DataFrame
            A DataFrame containing full contents of the requested ArcGIS sheet
        """
        # Concat data
        return pd.concat(
            [self.arcgis_json_to_df(x) for x in data], axis=0, ignore_index=True
        )


class SODA(StateDashboard, ABC):
    """
    This is to interact with SODA APIs

    Must define class variables:

    * `baseurl`

    in order to use this class
    """

    baseurl: str

    def __init__(
        self, execution_dt: pd.Timestamp, params: Optional[Dict[str, Any]] = None
    ):
        super(SODA, self).__init__()
        self.params = params

    def soda_query_url(
        self, data_id: str, resource: str = "resource", ftype: str = "json"
    ) -> str:
        """
        TODO fill this in

        Parameters
        ----------
        data_id :
        resource :
        ftype :

        Returns
        -------

        """
        out = self.baseurl + f"/{resource}/{data_id}.{ftype}"

        return out

    def get_dataset(
        self, data_id: str, resource: str = "resource", ftype: str = "json"
    ) -> pd.DataFrame:
        """
        TODO fill this in

        Parameters
        ----------
        data_id :
        resource :
        ftype :

        Returns
        -------

        """
        url = self.soda_query_url(data_id, resource, ftype)
        res = requests.get(url)

        df = pd.DataFrame(res.json())

        return df


class StateQueryAPI(StateDashboard, ABC):
    """
    Fetch data from OpenDataCali service
    """

    apiurl: str

    def count_current_records(self, res_json):
        """
        Determine the number of records in the single json

        Parameters
        ----------
        res_json : dict
            The json response

        Returns
        -------
        n : int
            The number of records in the json
        """
        return len(res_json["result"]["records"])

    def count_total_records(self, res_json):
        """
        Determine the number of records in the single json

        Parameters
        ----------
        res_json : dict
            The json response

        Returns
        -------
        n : int
            The total number of records in the endpoint
        """
        return res_json["result"]["total"]

    def extract_data_from_json(self, res_json):
        """
        Takes a json file and extracts the data elements

        Parameters
        ----------
        res_json : dict
            The json response

        Returns
        -------
        _ : list
            Each element of the list is an observation
        """
        return res_json["result"]["records"]

    def raw_from_api(self, resource_id: str, limit: int = 1000, **kwargs) -> List[Dict]:
        """
        Retrieves the raw data from the api. It assumes that data is
        stored in a json file as it is read in
        """
        # Create values needed for iterating
        offset = 0
        params = dict(resource_id=resource_id, limit=limit, offset=offset, **kwargs)

        # Store each json in a list
        the_jsons = []
        keep_requesting = True

        # Iterate on requests until we have all of the records
        while keep_requesting:
            res = requests.get(self.apiurl, params=params).json()
            if not res["success"]:
                raise ValueError("The request open CA data request failed...")

            # Append json info to the list
            the_jsons.append(res)

            # Extract relevant records
            records = res["result"]["records"]
            keep_requesting = offset < self.count_total_records(res)

            # Update offset
            offset += self.count_current_records(res)
            params["offset"] = offset

        return the_jsons

    def data_from_raw(self, data) -> pd.DataFrame:
        """
        Retrieves extracts data from the raw jsons (or other format)

        Parameters
        ----------
        data : dict
            The raw data

        Returns
        -------
        df: pd.DataFrame
            DataFrame with requested data
        """
        return pd.concat(
            [pd.DataFrame(self.extract_data_from_json(x)) for x in data],
            axis=0,
            ignore_index=True,
        )
