import json
import os
from typing import List, Union

import pandas as pd
import requests

from .. import DatasetBaseNoDate, InsertWithTempTable
from .census import ACSAPI
from .geo import _create_location

_VARIABLES_ACS = {
    "DP05_0001E": "Total population",
    "DP05_0018E": "Median age",
    "DP05_0024PE": "Fraction of population over 65",
    "DP05_0037PE": "Percent white",
    "DP05_0038PE": "Percent black",
    "DP05_0039PE": "Percent Native American or Alaska Native",
    "DP05_0044PE": "Percent Asian",
    "DP05_0052PE": "Percent Native Hawaiian or other Pacific Islander",
    "DP05_0057PE": "Percent other race",
    "DP05_0058PE": "Percent two or more races",
    "DP05_0071PE": "Percent Hispanic/Latino (any race)",
    "DP02_0059PE": "Percent of 25+ with less than 9th grade education",
    "DP02_0060PE": "Percent of 25+ with some HS but no HS degree",
    "DP02_0061PE": "Percent of 25+ with HS degree but no college",
    "DP02_0062PE": "Percent of 25+ with some college but no degree",
    "DP02_0063PE": "Percent of 25+ with Associate's degree",
    "DP02_0064PE": "Percent of 25+ with Bachelor's degree",
    "DP02_0065PE": "Percent of 25+ with professional degree",
    "DP03_0019PE": "Percent of 16+ commute driving alone",
    "DP03_0020PE": "Percent of 16+ commute carpool",
    "DP03_0021PE": "Percent of 16+ commute public transit",
    "DP03_0022PE": "Percent of 16+ commute walk",
    "DP03_0023PE": "Percent of 16+ commute other way",
    "DP03_0024PE": "Percent of 16+ commute work at home",
    "DP03_0025E": "Mean travel time to work (minutes)",
    # TODO: Add industries
    "DP03_0062E": "Median household income",
    "DP03_0063E": "Mean household income",
    "DP03_0096PE": "Percent of civilian population with health insurance",
    "DP03_0099PE": "Percent of civilian population with no health insurance",
    "DP03_0119PE": "Percent of families w/ income < poverty lvl in last year",
}


class ACS(ACSAPI, InsertWithTempTable, DatasetBaseNoDate):
    """
    Used to insert data and variable names into the database specified
    by schema.sql
    """

    table_name = "acs_data"
    pk = '("id", "fips")'
    autodag = False

    def __init__(
        self,
        cols: List[str] = ["DP05_0001E", "DP05_0018E", "DP05_0024PE"],
        geo: Union[list, str, dict] = "state",
        product: str = "acs1",
        table: str = "profile tables",
        year: int = 2018,
        key: str = os.environ.get("CENSUS_API_KEY", ""),
    ):
        if key == "":
            raise ValueError("`key` must be passed.")
        super(ACS, self).__init__(product=product, table=table, year=year, key=key)
        self.cols = cols
        self.geo = geo

    def _create_fips(self, df: pd.DataFrame):
        """
        Converts geographic columns into a fips code

        Parameters
        ----------
        df : pd.DataFrame
            The output of a `data_get` request and must include the
            relevant geographic columns

        Returns
        -------
        df : pd.DataFrame
            A DataFrame with the fips code values included and the
            other geographic columns dropped
        """
        df = _create_fips(self.geo, df)

        return df

    def _insert_query(self, df: pd.DataFrame, table_name: str, temp_name: str, pk: str):
        _sql_data_insert = f"""
        INSERT INTO data.{table_name} (id, fips, value)
        SELECT vt.id, tt.fips, tt.value FROM {temp_name} tt
        LEFT JOIN meta.acs_variables vt
          ON vt.census_id=tt.census_id AND
            vt.year={self.year} AND
            vt.product='{self.product}'
        ON CONFLICT {pk} DO UPDATE set value = excluded.value;
        """

        return _sql_data_insert

    def get(self):
        """
        Fetches the data for the variables provided in the `__init__`
        method from the specified ACS dataset

        Returns
        -------
        df : pd.DataFrame
            A DataFrame with a column for each variable requested and
            a column, `fips` which specifies the geographic information
        """
        # Fetch data
        df = super(ACS, self).get(self.cols, self.geo)

        # Convert to fips representation
        df = self._create_fips(df)

        # Reshape into desired format
        df = df.melt(id_vars="fips", var_name="census_id", value_name="value")

        return df


class ACSVariables(ACS, DatasetBaseNoDate):
    table_name = "acs_variables"
    pk = '("id")'

    def _insert_query(self, df: pd.DataFrame, table_name: str, temp_name: str, pk: str):
        _sql_var_insert = f"""
        INSERT INTO meta.{table_name} (year, product, census_id, label)
        SELECT year, product, census_id, label FROM {temp_name}
        ON CONFLICT (year, product, census_id) DO NOTHING;
        """

        return _sql_var_insert

    def get(self):
        """
        Fetches the variables for the specified ACS dataset

        Returns
        -------
        all_variables : pd.DataFrame
            A DataFrame with columns (year, product, census_id, label)
            which matches the columsn in the `uscensus.acs_variables`
            table
        """
        # Fetch variables json
        variable_json = json.loads(requests.get(self.dataset["c_variablesLink"]).text)

        # Load into DataFrame
        all_variables = pd.DataFrame.from_dict(variable_json["variables"]).T

        # Only keep 'Estimate' variables
        is_variable = all_variables["label"].str.contains("Estimate")
        all_variables = all_variables.loc[is_variable, ["label"]].reset_index()

        all_variables["year"] = self.dataset["c_vintage"]
        all_variables["product"] = self.product
        all_variables = all_variables.rename(columns={"index": "census_id"})

        return all_variables
