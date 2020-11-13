import textwrap
from abc import ABC

import pandas as pd
import requests
import us

from can_tools.scrapers.base import DatasetBaseNoDate, CMU
from can_tools.scrapers.official.base import StateDashboard


class OpenDataCali(StateDashboard, ABC):
    """
    Fetch data from OpenDataCali service
    """

    state_fips = int(us.states.lookup("California").fips)
    query_url = "https://data.ca.gov/api/3/action/datastore_search"

    def data_from_api(
        self, resource_id: str, limit: int = 1000, **kwargs
    ) -> pd.DataFrame:
        """

        Parameters
        ----------
        resource_id :
        limit :
        kwargs :

        Returns
        -------
        df: pd.DataFrame
            DataFrame with requested data

        TODO fill this in

        """
        # Create values needed for iterating
        offset = 0
        params = dict(resource_id=resource_id, limit=limit, offset=offset, **kwargs)

        dfs = []
        keep_requesting = True
        while keep_requesting:
            res = requests.get(self.query_url, params=params).json()
            if not res["success"]:
                raise ValueError("The request open CA data request failed...")

            records = res["result"]["records"]
            offset += len(records)
            keep_requesting = offset < res["result"]["total"]

            dfs.append(pd.DataFrame(records))
            params.update(dict(offset=offset))

        out = pd.concat(dfs, axis=0, ignore_index=True)

        return out


class California(DatasetBaseNoDate, OpenDataCali):
    """
    Fetch county level covid data from California state dashbaord
    """

    source = "https://covid19.ca.gov/state-dashboard"
    has_location = False

    def get_county_cases_deaths(self) -> pd.DataFrame:
        """
        Get cases and deaths from the OpenDataCali api

        Returns
        -------
        df: pd.DataFrame
            A pandas DataFrame containing cases and deaths for each county

        """
        # Set resource id and association dict
        resource_id = "926fd08f-cc91-4828-af38-bd45de97f8c3"
        crename = {
            "newcountconfirmed": CMU(
                category="cases", measurement="new", unit="people"
            ),
            "totalcountconfirmed": CMU(
                category="cases", measurement="cumulative", unit="people"
            ),
            "newcountdeaths": CMU(category="deaths", measurement="new", unit="people"),
            "totalcountdeaths": CMU(
                category="deaths", measurement="cumulative", unit="people"
            ),
        }

        # Read in data and convert to long format
        df = self.data_from_api(resource_id=resource_id)
        df["dt"] = pd.to_datetime(df["date"])

        df = df.melt(id_vars=["county", "dt"], value_vars=crename.keys()).dropna()

        # Determine the category of each observation
        df = self.extract_CMU(df, crename)

        cols_to_keep = [
            "dt",
            "county",
            "category",
            "measurement",
            "unit",
            "age",
            "race",
            "sex",
            "value",
        ]

        return df.loc[:, cols_to_keep]

    def get_hospital(self) -> pd.DataFrame:
        """
        Get icu and hospital usage by covid patients from the OpenDataCali api

        Returns
        -------
        df: pd.DataFrame
            A pandas DataFrame containing icu+hospital usage for each county

        """
        # Get url for download
        resource_id = "42d33765-20fd-44b8-a978-b083b7542225"
        df = self.data_from_api(resource_id=resource_id)

        # Convert column to date
        df = df.replace("None", None)
        df = df.apply(lambda x: pd.to_numeric(x, errors="ignore"))
        df["dt"] = pd.to_datetime(df["todays_date"])

        # Create a total number of icu covid patients
        df["icu_covid_patients"] = df.eval(
            "icu_covid_confirmed_patients + icu_suspected_covid_patients"
        )

        # Rename columns and subset data
        crename = {
            "hospitalized_covid_patients": CMU(
                category="hospital_beds_in_use_covid",
                measurement="current",
                unit="beds",
            ),
            "all_hospital_beds": CMU(
                category="hospital_beds_capacity", measurement="current", unit="beds"
            ),
            "icu_covid_patients": CMU(
                category="icu_beds_in_use_covid", measurement="current", unit="beds"
            ),
        }

        # Reshape
        out = df.melt(id_vars=["dt", "county"], value_vars=crename.keys()).dropna()

        # Determine the category and demographics of each observation
        out = self.extract_CMU(out, crename)

        cols_to_keep = [
            "dt",
            "county",
            "category",
            "measurement",
            "unit",
            "age",
            "race",
            "sex",
            "value",
        ]

        return out.loc[:, cols_to_keep]

    def get(self) -> pd.DataFrame:
        """
        Get all available CA COVID data from OpenDataCali services

        Returns
        -------
        df: pd.DataFrame
            A DataFrame with cases, deaths, icu COVID, hospital COVID for
            each county in CA
        """

        cases = self.get_county_cases_deaths()
        hospital = self.get_hospital()

        out = pd.concat([cases, hospital], axis=0, ignore_index=True, sort=True)
        out["vintage"] = self._retrieve_vintage()

        return out
