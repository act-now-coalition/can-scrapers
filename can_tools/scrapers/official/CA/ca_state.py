from typing import Any

import pandas as pd
import us

from can_tools.scrapers.base import CMU
from can_tools.scrapers.official.base import StateQueryAPI

# https://data.chhs.ca.gov/api/3/action/datastore_search?resource_id=046cdd2b-31e5-4d34-9ed3-b48cdbc4be7a&limit=5
# https://data.ca.gov/api/3/action/datastore_search?resource_id=0545b878-90a8-4b26-a16a-84978354f1a3&limit=5

# https://data.chhs.ca.gov/api/3/action/datastore_search?resource_id=046cdd2b-31e5-4d34-9ed3-b48cdbc4be7a&limit=5
class CaliforniaCasesDeaths(StateQueryAPI):
    """
    Fetch county level covid data from California state dashbaord
    """

    apiurl = "https://data.chhs.ca.gov/api/3/action/datastore_search"
    # apiurl = "https://data.ca.gov/api/3/action/datastore_search"
    source = "https://covid19.ca.gov/state-dashboard"
    source_name = "Official California State Government Website"
    state_fips = int(us.states.lookup("California").fips)
    has_location = False
    location_type = "county"
    resource_id = "046cdd2b-31e5-4d34-9ed3-b48cdbc4be7a"

    def fetch(self) -> Any:
        return self.raw_from_api(self.resource_id, limit=1000)

    def pre_normalize(self, data) -> pd.DataFrame:
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
        df = self.data_from_raw(data).rename(columns={"county": "location_name"})
        df["dt"] = pd.to_datetime(df["date"])

        # Move things into long format
        df = df.melt(
            id_vars=["location_name", "dt"], value_vars=crename.keys()
        ).dropna()

        # Determine the category of each observation
        df = self.extract_CMU(df, crename)

        cols_to_keep = [
            "dt",
            "location_name",
            "category",
            "measurement",
            "unit",
            "age",
            "race",
            "ethnicity",
            "sex",
            "value",
        ]
        return df.loc[:, cols_to_keep]

    def normalize(self, data) -> pd.DataFrame:
        # Normalize case/death and hospital data
        out = self.pre_normalize(data)
        out["vintage"] = self._retrieve_vintage()

        # Drop the information that we won't be keeping track of
        loc_not_keep = ["Out Of Country", "Unassigned", "Unknown"]
        out = out.loc[~out["location_name"].isin(loc_not_keep), :]

        return out


class CaliforniaHospitals(CaliforniaCasesDeaths):
    resource_id = "0d9be83b-5027-41ff-97b2-6ca70238d778"
    apiurl = "https://data.ca.gov/api/3/action/datastore_search"

    def pre_normalize(self, data) -> pd.DataFrame:
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
        df = self.data_from_raw(data).rename(columns={"county": "location_name"})

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
            id_vars=["dt", "location_name"], value_vars=crename.keys()
        ).dropna()

        # Determine the category and demographics of each observation
        out = self.extract_CMU(out, crename)

        cols_to_keep = [
            "dt",
            "location_name",
            "category",
            "measurement",
            "unit",
            "age",
            "race",
            "ethnicity",
            "sex",
            "value",
        ]

        return out.loc[:, cols_to_keep]
