import json
import uuid
import re

from abc import ABC, abstractmethod
from base64 import b64decode
from contextlib import closing
from typing import Any, Dict, List, Optional, Tuple, Type, Union
from urllib.parse import urlparse, parse_qs

import pandas as pd
import requests
import urllib.parse

from bs4 import BeautifulSoup
from sqlalchemy.engine.base import Engine
from sqlalchemy.orm.session import sessionmaker

from can_tools.db_util import fast_append_to_sql
from can_tools.models import (
    Base,
    CovidObservation,
    TemptableOfficialHasLocation,
    TemptableOfficialNoLocation,
    build_insert_from_temp,
)
from can_tools.scrapers.base import DatasetBase
from can_tools.scrapers.util import requests_retry_session


class StateDashboard(DatasetBase, ABC):
    """
    Definition of common parameters and values for scraping a State Dashboard

    Attributes
    ----------

    table: Type[Base] = CovidObservation
        SQLAlchemy base table to insert into
    provider = "state"
        Provider here is state
    data_type: str = "covid"
        Data type is set to covid
    has_location: bool
        Must be set by subclasses. True if location code (fips code) appears in data
    state_fips: int
        Must be set by subclasses. The two digit state fips code (as an int)

    """

    table: Type[Base] = CovidObservation
    provider = "state"
    data_type: str = "covid"
    has_location: bool
    state_fips: int
    location_type: str
    source: str

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

        if "source_url" not in list(to_ins):
            to_ins["source_url"] = self.source

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
        rows_inserted = 0
        rows_deleted = 0
        with closing(sessionmaker(engine)()) as sess:
            try:
                fast_append_to_sql(to_ins, engine, table)
                print("Inserted all rows to temp table")

                # then insert from temp table
                ins = build_insert_from_temp(insert_op, table, engine)
                res = sess.execute(ins)
                sess.commit()
                rows_inserted = res.rowcount
                print("Inserted {} rows".format(rows_inserted))
                worked = True
            finally:
                deleter = table.__table__.delete().where(table.insert_op == insert_op)
                res_delete = sess.execute(deleter)
                sess.commit()
                rows_deleted = res_delete.rowcount
                print("Removed the {} rows from temp table".format(rows_deleted))

        return worked, rows_inserted, rows_deleted


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

        if "source_url" not in list(to_ins):
            to_ins["source_url"] = self.source

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
        self,
        execution_dt: pd.Timestamp = pd.Timestamp.utcnow(),
        params: Optional[Dict[str, Any]] = None,
    ):
        super(SODA, self).__init__(execution_dt)
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


class TableauDashboard(StateDashboard, ABC):
    """
    Fetch data from a Tableau dashboard

    Must define class variables:

    * `baseurl`
    * `viewPath`

    in order to use this class.

    If the dashboard requires a filter, define:

    * `filterFunctionName`
    * `filterFunctionValue`

    to drill down on values like counties, dates, etc.

    """

    baseurl: str
    viewPath: str
    filterFunctionName: Optional[str] = None
    filterFunctionValue: Optional[str] = None

    def get_tableau_view(self):
        def onAlias(it, value, cstring):
            return value[it] if (it >= 0) else cstring["dataValues"][abs(it) - 1]

        req = requests_retry_session()
        fullURL = self.baseurl + "/views/" + self.viewPath
        if self.filterFunctionName is not None:
            params = ":language=en&:display_count=y&:origin=viz_share_link&:embed=y&:showVizHome=n&:jsdebug=y&"
            params += self.filterFunctionName + "=" + self.filterFunctionValue
            reqg = req.get(fullURL, params=params)
        else:
            reqg = req.get(
                fullURL,
                params={
                    ":language": "en",
                    ":display_count": "y",
                    ":origin": "viz_share_link",
                    ":embed": "y",
                    ":showVizHome": "n",
                    ":jsdebug": "y",
                    ":apiID": "host4",
                    "#navType": "1",
                    "navSrc": "Parse",
                },
                headers={"Accept": "text/javascript"},
            )
        soup = BeautifulSoup(reqg.text, "html.parser")
        tableauTag = soup.find("textarea", {"id": "tsConfigContainer"})
        tableauData = json.loads(tableauTag.text)
        parsed_url = urllib.parse.urlparse(fullURL)
        dataUrl = f'{parsed_url.scheme}://{parsed_url.hostname}{tableauData["vizql_root"]}/bootstrapSession/sessions/{tableauData["sessionid"]}'

        # copy over some additional headers from tableauData
        form_data = {}
        form_map = {
            "sheetId": "sheet_id",
            "showParams": "showParams",
            "stickySessionKey": "stickySessionKey",
        }
        for k, v in form_map.items():
            if k in tableauData:
                form_data[v] = tableauData[k]

        resp = req.post(
            dataUrl,
            data=form_data,
            headers={"Accept": "text/javascript"},
        )
        # Parse the response.
        # The response contains multiple chuncks of the form
        # `<size>;<json>` where `<size>` is the number of bytes in `<json>`
        resp_text = resp.text
        data = []
        while len(resp_text) != 0:
            size, rest = resp_text.split(";", 1)
            chunck = json.loads(rest[: int(size)])
            data.append(chunck)
            resp_text = rest[int(size) :]

        # The following section (to the end of the method) uses code from
        # https://stackoverflow.com/questions/64094560/how-do-i-scrape-tableau-data-from-website-into-r
        presModel = data[1]["secondaryInfo"]["presModelMap"]
        metricInfo = presModel["vizData"]["presModelHolder"]
        metricInfo = metricInfo["genPresModelMapPresModel"]["presModelMap"]
        data = presModel["dataDictionary"]["presModelHolder"]
        data = data["genDataDictionaryPresModel"]["dataSegments"]["0"]["dataColumns"]

        scrapedData = {}

        for metric in metricInfo:
            metricsDict = metricInfo[metric]["presModelHolder"]["genVizDataPresModel"]
            columnsData = metricsDict["paneColumnsData"]

            result = [
                {
                    "fieldCaption": t.get("fieldCaption", ""),
                    "valueIndices": columnsData["paneColumnsList"][t["paneIndices"][0]][
                        "vizPaneColumns"
                    ][t["columnIndices"][0]]["valueIndices"],
                    "aliasIndices": columnsData["paneColumnsList"][t["paneIndices"][0]][
                        "vizPaneColumns"
                    ][t["columnIndices"][0]]["aliasIndices"],
                    "dataType": t.get("dataType"),
                    "paneIndices": t["paneIndices"][0],
                    "columnIndices": t["columnIndices"][0],
                }
                for t in columnsData["vizDataColumns"]
                if t.get("fieldCaption")
            ]
            frameData = {}
            cstring = [t for t in data if t["dataType"] == "cstring"][0]
            for t in data:
                for index in result:
                    if t["dataType"] == index["dataType"]:
                        if len(index["valueIndices"]) > 0:
                            frameData[f'{index["fieldCaption"]}-value'] = [
                                t["dataValues"][abs(it)] for it in index["valueIndices"]
                            ]
                        if len(index["aliasIndices"]) > 0:
                            frameData[f'{index["fieldCaption"]}-alias'] = [
                                onAlias(it, t["dataValues"], cstring)
                                for it in index["aliasIndices"]
                            ]

            df = pd.DataFrame.from_dict(frameData, orient="index").fillna(0).T

            scrapedData[metric] = df

        return scrapedData


class TableauMapClick(StateDashboard, ABC):
    """
    Defines a few commonly-used helper methods for snagging Tableau data
    from mapclick-driven dashboard pages specifically
    """

    def getTbluMapFilter(self, htmDump) -> List:
        """
        Extracts the onMapClick background data filter function from a raw tableau HTML bootstrap return

        Parameters
        ----------
        htmdump : json
            The raw json-ized output of the info field from getRawTbluPageData

        Returns
        -------
        _ : List
            The Tableau-view-specific json filter function called onMapClick
        """
        urlFltr = []
        # Grab the map filter function guts:
        for fn in htmDump["worldUpdate"]["applicationPresModel"]["workbookPresModel"][
            "dashboardPresModel"
        ]["userActions"]:
            if fn.get("name") == "Map filter":
                urlFltr = (
                    urllib.parse.unquote(fn.get("linkSpec").get("url"))
                    .split("?")[1]
                    .replace("=<Countynm1~na>", "")
                    .split("&")
                )
        return urlFltr

    def extractTbluData(self, htmdump, area) -> pd.DataFrame:
        """
        Extracts data from raw tableau HTML bootstrap return

        Parameters
        ----------
        htmdump : json
            The raw json-ized output of the fdat data field from getRawTbluPageData

        area : the FIPS code of the htmdump

        Returns
        -------
        _ : pd.DataFrame
            Already-pivoted covid data with column names extracted from tableau
            'location' column will contain the proveded 'area' data
        """
        valF = []  # Initialize placeholder array
        # Grab the raw data loaded into the current tableau view
        lsUpdt = htmdump["secondaryInfo"]["presModelMap"]["dataDictionary"][
            "presModelHolder"
        ]["genDataDictionaryPresModel"]["dataSegments"]["0"]["dataColumns"][2][
            "dataValues"
        ][
            -1
        ]
        intDat = htmdump["secondaryInfo"]["presModelMap"]["dataDictionary"][
            "presModelHolder"
        ]["genDataDictionaryPresModel"]["dataSegments"]["0"]["dataColumns"][0][
            "dataValues"
        ]
        rlDat = htmdump["secondaryInfo"]["presModelMap"]["dataDictionary"][
            "presModelHolder"
        ]["genDataDictionaryPresModel"]["dataSegments"]["0"]["dataColumns"][1][
            "dataValues"
        ]
        # First extract the datatype and indices:
        for i in htmdump["secondaryInfo"]["presModelMap"]["vizData"]["presModelHolder"][
            "genPresModelMapPresModel"
        ]["presModelMap"]:
            dtyp = htmdump["secondaryInfo"]["presModelMap"]["vizData"][
                "presModelHolder"
            ]["genPresModelMapPresModel"]["presModelMap"][i]["presModelHolder"][
                "genVizDataPresModel"
            ][
                "paneColumnsData"
            ][
                "vizDataColumns"
            ][
                1
            ].get(
                "dataType"
            )
            indx = htmdump["secondaryInfo"]["presModelMap"]["vizData"][
                "presModelHolder"
            ]["genPresModelMapPresModel"]["presModelMap"][i]["presModelHolder"][
                "genVizDataPresModel"
            ][
                "paneColumnsData"
            ][
                "paneColumnsList"
            ][
                0
            ][
                "vizPaneColumns"
            ][
                1
            ].get(
                "aliasIndices"
            )[
                0
            ]
            if dtyp == "integer":
                valF.append([area, i, intDat[indx]])
            elif dtyp == "real":
                valF.append([area, i, rlDat[indx]])
        valF.append([area, "Last update", lsUpdt])
        if valF:
            val = pd.DataFrame(valF, columns=["location", "Name", "Value"])
            val = pd.pivot_table(
                val,
                values="Value",
                index=["location"],
                columns="Name",
                aggfunc="first",
            ).reset_index()
            return val
        else:
            return None

    def getRawTbluPageData(self, url, bsRt, reqParams) -> (json, json):
        """
        Extracts and parses htm data from a tableau dashboard page

        Parameters
        ----------
        url : str
            The root of the Tableau dashboard

        bsRt : str
            The bootstrap root url.
            Typically everything before the first '/' delimiter in 'url'

        reqParams : dict
            Dictionary of request parameters useable by 'requests' library

        Returns
        -------
        info, fdat : (json, json)
            'info' is the header section of the Tableau dashboard page (as json)
            'fdat' is the data section of the Tableau dashboard page (as json)
        """
        # Initialize main page: grab session ID key, sheet ID key, root directory string
        r = requests.get(url, params=reqParams)

        # Parse the output, return a json so we can build a bootstrap call
        suppe = BeautifulSoup(r.text, features="lxml")
        tdata = json.loads(suppe.find("textarea", {"id": "tsConfigContainer"}).text)

        # Call the bootstrapper: grab the state data, map selection update function
        dataUrl = f'{bsRt}{tdata["vizql_root"]}/bootstrapSession/sessions/{tdata["sessionid"]}'
        r = requests.post(
            dataUrl,
            data={"sheet_id": tdata["sheetId"], "showParams": tdata["showParams"]},
        )

        # Regex the non-json output
        dat = re.search("\d+;({.*})\d+;({.*})", r.text, re.MULTILINE)

        # load info head and data group separately
        info = json.loads(dat.group(1))
        fdat = json.loads(dat.group(2))

        return (info, fdat)


class MicrosoftBIDashboard(StateDashboard, ABC):
    powerbi_url: str

    def __init__(self, *a, **kw):
        super(MicrosoftBIDashboard, self).__init__(*a, **kw)
        self._sess = None

    @property
    def sess(self):
        if self._sess is not None:
            return self._sess
        else:
            self._setup_sess()

    def _setup_sess(self):

        self._sess = requests.Session()
        self._sess.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64)",
            }
        )
        self.source_res = self._sess.get(self.source)
        self.source_soup = BeautifulSoup(self.source_res.content, features="lxml")

    def powerbi_models_url(self, rk):
        return (
            self.powerbi_url
            + f"/public/reports/{rk}/modelsAndExploration?preferReadOnlySession=true"
        )

    def powerbi_query_url(self):
        return self.powerbi_url + "/public/reports/querydata?synchronous=true"

    def get_dashboard_iframe(self):
        "This method assumes that there is only one PowerBI iframe..."
        source_iframes = self.source_soup.find_all("iframe")
        dashboard_frame = [f for f in source_iframes if "powerbi" in f["src"]][0]

        return dashboard_frame

    def get_resource_key(self, dashboard_frame):
        "Decodes the resource key using the dashboard iframe (and it's link)"
        # The resource key is base64 encoded in the argument to the url...
        parsed_url = urlparse(dashboard_frame["src"])
        args = parse_qs(parsed_url.query)
        resource_key = json.loads(b64decode(args["r"][0]))["k"]

        return resource_key

    def get_model_data(self, resource_key):
        # Get headers
        headers = self.construct_headers(resource_key)

        # Create the model url and make GET request
        model_url = self.powerbi_models_url(resource_key)
        model_res = self.sess.get(model_url, headers=headers)
        model_data = json.loads(model_res.content)

        # Extract relevant info
        ds_id = model_data["models"][0]["dbName"]
        model_id = model_data["models"][0]["id"]
        report_id = model_data["exploration"]["report"]["objectId"]

        return ds_id, model_id, report_id

    def construct_headers(self, resource_key):
        # Dictionary to fill
        headers = {}

        # Get the activity id
        # activity_id = self.source_res.headers["request-id"]
        # headers["RequestId"] = activity_id

        # Get the resource key
        headers["X-PowerBI-ResourceKey"] = resource_key

        return headers

    def construct_from(self, nets):
        """
        Constructs the from component of the PowerBI query

        Parameters
        ----------
        nets : list(tuple)
            A list of tuples containing "Name", "Entity", and "Type"
            information for each source
        """
        # Must have at least one source
        assert len(nets) >= 1

        out = []
        for (n, e, t) in nets:
            out.append({"Name": n, "Entity": e, "Type": t})

        return out

    def construct_select(self, sels, aggs, meas):
        """
        Constructs the select component of the PowerBI query

        Parameters
        ----------
        sels : list(tuple)
            A list of tuples containing information on the "Source" (should
            match "Name" from the `construct_from` method), "Property", and
            "Name". This is for columns that are directly selected rather
            than aggregated
        aggs : list(tuple)
            A list of tuples containing information on the "Source" (should
            match "Name" from the `construct_from` method), "Property",
            "Function", and "Name". This is for columns that are aggregated
        meas : list(tuple)
            A list of tuples containing information on the "Source", "Property",
            and "Name". I don't know exactly the difference between `sels` and
            `meas` but they differ slightly
        """
        assert len
        out = []

        for (s, p, n) in sels:
            out.append(
                {
                    "Column": {
                        "Expression": {"SourceRef": {"Source": s}},
                        "Property": p,
                    },
                    "Name": n,
                }
            )

        for (s, p, f, n) in aggs:
            out.append(
                {
                    "Aggregation": {
                        "Expression": {
                            "Column": {
                                "Expression": {"SourceRef": {"Source": s}},
                                "Property": p,
                            }
                        },
                        "Function": f,
                    },
                    "Name": n,
                }
            )

        for (s, p, n) in meas:
            out.append(
                {
                    "Measure": {
                        "Expression": {"SourceRef": {"Source": s}},
                        "Property": p,
                    },
                    "Name": n,
                }
            )

        return out

    def construct_application_context(self, ds_id, report_id):
        out = {"DatasetId": ds_id, "Sources": [{"ReportId": report_id}]}
        return out

    @abstractmethod
    def construct_body(self):
        pass
