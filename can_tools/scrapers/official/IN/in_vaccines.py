from typing import Any

import pandas as pd
import us

from can_tools.scrapers import variables
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
        cmus = {
            "fully_vaccinated": variables.FULLY_VACCINATED_ALL,
            "at_least_one_dose": variables.INITIATING_VACCINATIONS_ALL,
        }

        # Read in data and convert to long format
        df = self.data_from_raw(data)

        # Drop unwanted locations
        unwanted_loc = ["Unknown", "Out of State"]
        df = df.query("fips not in @unwanted_loc")
        df["dt"] = pd.to_datetime(df["date"])
        df["location"] = df["fips"].astype(int)

        column_subset = [
            "first_dose_administered",
            "fully_vaccinated",
            "single_dose_administered",
            "second_dose_administered",
        ]
        return (
            df.set_index(["dt", "location"])
            .loc[:, column_subset]
            .unstack(level="location")
            .sort_index()
            .cumsum()
            .stack(level="location")
            .assign(
                at_least_one_dose=lambda x: x.eval(
                    "first_dose_administered + single_dose_administered"
                )
            )
            .drop(
                [
                    "first_dose_administered",
                    "second_dose_administered",
                    "single_dose_administered",
                ],
                axis="columns",
            )
            .astype(int)
            .rename_axis(columns=["variable"])
            .stack()
            .rename("value")
            .reset_index()
            .pipe(self.extract_CMU, cmu=cmus)
            .assign(vintage=self._retrieve_vintage())
        )
