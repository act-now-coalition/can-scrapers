from typing import Any

import pandas as pd
import us

from can_tools.scrapers.base import CMU
from can_tools.scrapers.official.base import StateQueryAPI
class CaliforniaHospitals(StateQueryAPI):
    resource_id = "0d9be83b-5027-41ff-97b2-6ca70238d778"
    apiurl = "https://data.ca.gov/api/3/action/datastore_search"
    state_fips = int(us.states.lookup("California").fips)
    source_name = "Official California State Government Website"
    source = "https://covid19.ca.gov/state-dashboard"
    location_type = "county"
    has_location = False

    def fetch(self) -> Any:
        return self.raw_from_api(self.resource_id, limit=1000)

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

    def normalize(self, data) -> pd.DataFrame:
        # Normalize case/death and hospital data
        out = self.pre_normalize(data)
        out["vintage"] = self._retrieve_vintage()

        # Drop the information that we won't be keeping track of
        loc_not_keep = ["Out Of Country", "Unassigned", "Unknown"]
        out = out.loc[~out["location_name"].isin(loc_not_keep), :]

        return out
