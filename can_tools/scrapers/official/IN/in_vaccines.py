from typing import Any

import pandas as pd
import us

from can_tools.scrapers.base import CMU
from can_tools.scrapers.official.base import StateQueryAPI


class IndianaCountyVaccinations(StateQueryAPI):
    """
    Fetch county level covid data from California state dashbaord
    """

    apiurl = "https://hub.mph.in.gov/api/3/action/datastore_search"
    source = "https://www.coronavirus.in.gov/vaccine/2680.htm"
    source_name = "Official Indiana State Government Website"
    state_fips = int(us.states.lookup("Indiana").fips)
    has_location = True
    location_type = "county"
    resource_id = "a4d23ae8-34c2-4951-85e0-3ea9345ee6ea"

    def fetch(self) -> Any:
        return self.raw_from_api(self.resource_id, limit=1000)

    def normalize(self, data) -> pd.DataFrame:
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
            "first_dose_administered": CMU(
                category="total_vaccine_initiated",
                measurement="cumulative",
                unit="people",
            ),
            "fully_vaccinated": CMU(
                category="total_vaccine_completed",
                measurement="cumulative",
                unit="people",
            ),
            "new_first_dose_administered": CMU(
                category="total_vaccine_initiated", measurement="new", unit="people"
            ),
            "new_fully_vaccinated": CMU(
                category="total_vaccine_completed", measurement="new", unit="people"
            ),
        }

        # Read in data and convert to long format
        df = self.data_from_raw(data)

        # Drop unwanted locations
        unwanted_loc = ["Unknown", "Out of State"]
        df = df.query("fips not in @unwanted_loc")

        # Set date and convert fips to int
        df["dt"] = pd.to_datetime(df["date"])
        df["location"] = pd.to_numeric(df["fips"])

        # Move things into long format
        df = df.melt(id_vars=["location", "dt"], value_vars=crename.keys()).dropna()

        # Determine the category of each observation
        out = self.extract_CMU(df, crename)

        cols_to_keep = [
            "dt",
            "location",
            "category",
            "measurement",
            "unit",
            "age",
            "race",
            "ethnicity",
            "sex",
            "value",
        ]
        out["vintage"] = self._retrieve_vintage()

        return out
