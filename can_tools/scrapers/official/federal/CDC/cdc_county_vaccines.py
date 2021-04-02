import random

import pandas as pd
import requests
import us

from can_tools.scrapers.base import ALL_STATES_PLUS_DC, CMU
from can_tools.scrapers.official.base import FederalDashboard


class CDCCountyVaccine(FederalDashboard):
    has_location = True
    location_type = "county"
    source = "https://covid.cdc.gov/covid-data-tracker/#county-view"
    source_name = "Centers for Disease Control and Prevention"
    provider = "cdc"

    def fetch(self, test=False):
        fetcher_url = (
            "https://covid.cdc.gov/covid-data-tracker/COVIDData/"
            "getAjaxData?id=vaccination_county_condensed_data"
        )
        response = requests.get(fetcher_url)

        return response.json()

    def normalize(self, data):
        # Read data in
        df = pd.DataFrame.from_records(data["vaccination_county_condensed_data"])

        # Set date
        df["dt"] = pd.to_datetime(df["Date"])

        # Set fips code
        df["location"] = pd.to_numeric(df["FIPS"])

        crename = {
            "Series_Complete_Yes": CMU(
                category="total_vaccine_completed",
                measurement="cumulative",
                unit="people",
            ),
            "Series_Complete_18Plus": CMU(
                category="total_vaccine_completed",
                measurement="cumulative",
                unit="people",
                age="18_plus",
            ),
            "Series_Complete_65Plus": CMU(
                category="total_vaccine_completed",
                measurement="cumulative",
                unit="people",
                age="65_plus",
            ),
            "Series_Complete_Pop_Pct": CMU(
                category="total_vaccine_completed",
                measurement="current",
                unit="percentage",
            ),
            "Series_Complete_18PlusPop_Pct": CMU(
                category="total_vaccine_completed",
                measurement="current",
                unit="percentage",
                age="18_plus",
            ),
            "Series_Complete_65PlusPop_Pct": CMU(
                category="total_vaccine_completed",
                measurement="current",
                unit="percentage",
                age="65_plus",
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
