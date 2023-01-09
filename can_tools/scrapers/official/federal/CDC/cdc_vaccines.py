import random

import pandas as pd
import requests
import us

from can_tools.scrapers import variables
from can_tools.scrapers.base import ALL_STATES_PLUS_TERRITORIES, CMU
from can_tools.scrapers.official.base import FederalDashboard


def _lookup(location):
    if location == "US":
        return 0
    return int(us.states.lookup(location).fips)


class CDCStateVaccine(FederalDashboard):
    has_location = True
    location_type = "state"
    source = "https://data.cdc.gov/Vaccinations/COVID-19-Vaccinations-in-the-United-States-Jurisdi/unsk-b7fc"
    source_name = "Centers for Disease Control and Prevention"
    provider = "cdc"

    variables = {
        "Distributed": variables.TOTAL_VACCINE_DISTRIBUTED,
        "Administered_Dose1_Recip": variables.INITIATING_VACCINATIONS_ALL,
        "Series_Complete_Yes": variables.FULLY_VACCINATED_ALL,
        "Administered": variables.TOTAL_DOSES_ADMINISTERED_ALL,
        "Additional_Doses": variables.PEOPLE_VACCINATED_ADDITIONAL_DOSE,
        # CDC has authorized bivalent vaccines for people as young as 6 months
        # but the CDC doesn't appear to have data for this age group
        # https://www.fda.gov/news-events/press-announcements/coronavirus-covid-19-update-fda-authorizes-updated-bivalent-covid-19-vaccines-children-down-6-months
        "Bivalent_Booster_5Plus": variables.PEOPLE_VACCINATED_BIVALENT_DOSE
    }

    def fetch(self, test=False):
        return pd.read_csv(
            "https://data.cdc.gov/api/views/unsk-b7fc/rows.csv?accessType=DOWNLOAD"
        )

    def _filter_rows(self, df):
        state_abbr_list = [x.abbr for x in ALL_STATES_PLUS_TERRITORIES]
        return df.loc[df["Location"].isin(state_abbr_list), :]

    def normalize(self, df):
        # Set date
        df["dt"] = pd.to_datetime(df["Date"])

        # Only keep states and set fips codes
        df = self._filter_rows(df)
        df.loc[:, "location"] = df["Location"].map(_lookup)

        return self._reshape_variables(df, self.variables)


class CDCUSAVaccine(CDCStateVaccine):
    location_type = "nation"

    def _filter_rows(self, df):
        return df.query("Location == 'US'")


def one_time_backfill_usa_vaccine():
    df = pd.read_csv(
        "/home/sglyon/Downloads/trends_in_number_of_covid19_vaccinations_in_the_us.csv",
        skiprows=2,
    )
    filtered = df.loc[(df["Date Type"] == "Admin") & (df["Program"] == "US")]
    variable_map = {
        "People with at least One Dose Cumulative": variables.INITIATING_VACCINATIONS_ALL,
        "People Fully Vaccinated Cumulative": variables.FULLY_VACCINATED_ALL,
    }

    d = CDCUSAVaccine()
    cols = list(variable_map.keys()) + ["Date"]
    df = (
        filtered.loc[:, cols]
        .assign(location=0)
        .pipe(
            d._rename_or_add_date_and_location,
            location_column="location",
            date_column="Date",
        )
        .pipe(d._reshape_variables, variable_map)
    )
    return df
