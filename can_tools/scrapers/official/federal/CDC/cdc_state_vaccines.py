import random
import requests

import pandas as pd
import us

from can_tools.scrapers.base import CMU, ALL_STATES_PLUS_DC
from can_tools.scrapers.official.base import FederalDashboard


class CDCStateVaccine(FederalDashboard):
    has_location = True
    location_type = "state"
    source = "https://covid.cdc.gov/covid-data-tracker/#vaccinations"
    provider = "cdc"

    def fetch(self, test=False):
        fetcher_url = (
            "https://covid.cdc.gov/covid-data-tracker/COVIDData/"
            "getAjaxData?id=vaccination_data"
        )
        response = requests.get(fetcher_url)

        return response.json()

    def normalize(self, data):
        # Read data in
        df = pd.DataFrame.from_records(data["vaccination_data"])

        # Set date
        df["dt"] = pd.to_datetime(df["Date"])

        # Only keep states and set fips codes
        state_abbr_list = [x.abbr for x in ALL_STATES_PLUS_DC]
        df = df.loc[df["Location"].isin(state_abbr_list), :]
        df.loc[:, "location"] = df["Location"].map(
            lambda x: int(us.states.lookup(x).fips)
        )

        crename = {
            "Doses_Distributed": CMU(
                category="total_vaccine_distributed",
                measurement="cumulative",
                unit="doses",
            ),
            "Administered_Dose1": CMU(
                category="total_vaccine_initiated",
                measurement="cumulative",
                unit="people",
            ),
            "Administered_Dose2": CMU(
                category="total_vaccine_completed",
                measurement="cumulative",
                unit="people",
            ),
            "Doses_Administered": CMU(
                category="total_vaccine_doses_administered",
                measurement="cumulative",
                unit="doses",
            ),
        }

        # Reshape and add variable information
        out = df.melt(id_vars=["dt", "location"], value_vars=crename.keys()).dropna()
        out = self.extract_CMU(out, crename)
        out["vintage"] = self._retrieve_vintage()

        cols_2_keep = [
            "vintage",
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
        return out.loc[:, cols_2_keep]
