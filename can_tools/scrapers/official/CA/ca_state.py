import textwrap
from abc import ABC

import pandas as pd
import requests
import us

from typing import Any, Dict, List, Optional, Union

from can_tools.scrapers.base import DatasetBase, CMU
from can_tools.scrapers.official.base import StateDashboard, StateQueryAPI


class California(StateQueryAPI, DatasetBase):
    """
    Fetch county level covid data from California state dashbaord
    """
    apiurl = "https://data.ca.gov/api/3/action/datastore_search"
    source = "https://covid19.ca.gov/state-dashboard"
    state_fips = int(us.states.lookup("California").fips)
    has_location = False

    def fetch(self) -> Any:
        # Dictionary for storing the raw data
        raw_data = {}

        # case and death data
        resource_id_cd = "926fd08f-cc91-4828-af38-bd45de97f8c3"
        raw_data["cases_deaths"] = self.raw_from_api(resource_id_cd, limit=1000)

        # case and death data
        resource_id_h = "42d33765-20fd-44b8-a978-b083b7542225"
        raw_data["hospitals"] = self.raw_from_api(resource_id_h, limit=1000)

        return raw_data

    def normalize_cases_deaths(self, data) -> pd.DataFrame:
        """
        Normalizes the list of json objects that corresponds with case
        and death data

        Parameters
        ----------
        data : List
            A list of json elements

        Returns
        -------
        df : pd.DataFrame
            A DataFrame with the normalized data
        """
        # Map current column names to CMU elements
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
        df = self.data_from_raw(data)
        df["dt"] = pd.to_datetime(df["date"])

        # Move things into long format
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

    def normalize_hospitals(self, data) -> pd.DataFrame:
        """
        Get icu and hospital usage by covid patients from the OpenDataCali api

        Parameters
        ----------
        data : List
            A list of json elements

        Returns
        -------
        df: pd.DataFrame
            A pandas DataFrame containing icu+hospital usage for each county

        """
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

        # Read in data and convert to long format
        df = self.data_from_raw(data)

        # Convert column to date
        df = df.replace("None", None)
        df = df.apply(lambda x: pd.to_numeric(x, errors="ignore"))
        df["dt"] = pd.to_datetime(df["todays_date"])

        # Create a total number of icu covid patients
        df["icu_covid_patients"] = df.eval(
            "icu_covid_confirmed_patients + icu_suspected_covid_patients"
        )

        # Reshape
        out = df.melt(
            id_vars=["dt", "county"], value_vars=crename.keys()
        ).dropna()

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

    def normalize(self, data) -> pd.DataFrame:
        # Normalize case/death and hospital data
        cases_deaths = self.normalize_cases_deaths(data["cases_deaths"])
        hospitals = self.normalize_hospitals(data["hospitals"])

        out = pd.concat(
            [cases_deaths, hospitals], axis=0, ignore_index=True, sort=True
        )
        out["vintage"] = self._retrieve_vintage()

        return out

    def validate(self, df, df_hist) -> bool:
        return True
