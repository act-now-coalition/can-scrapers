import us
from bs4 import BeautifulSoup
import re
import json
import os
import pandas as pd
import requests

from can_tools.scrapers import variables
from can_tools.scrapers.official.base import (
    TableauDashboard,
    TableauMapClick,
)

from can_tools.scrapers.util import requests_retry_session


class OregonVaccineCounty(TableauDashboard):
    has_location = False
    source = "https://covidvaccine.oregon.gov/"
    source_name = "Oregon Health Authority"
    state_fips = int(us.states.lookup("Oregon").fips)
    location_type = "county"

    baseurl = "https://public.tableau.com/"
    viewPath = "OregonCOVID-19VaccinationTrends/OregonCountyVaccinationTrends"

    cmus = {
        "SUM(People Count)-alias": variables.INITIATING_VACCINATIONS_ALL,
        "SUM(Vaccinated)-alias": variables.FULLY_VACCINATED_ALL,
        "SUM(Administrations)-alias": variables.TOTAL_DOSES_ADMINISTERED_ALL,
    }

    location_name_col = "AGG(County People Count Label)-alias"
    timezone = "US/Pacific"
    filterFunctionName = "[federated.0t5ugmz0hnw7q719jeh0615iizas].[none:county:nk]"

    def fetch(self):
        path = os.path.dirname(__file__) + "/../../../bootstrap_data/locations.csv"
        counties = list(
            pd.read_csv(path).query(f"state == 41 and location != 41")["name"]
        )

        results = {}
        for county in counties:
            print("making request for: ", county)
            self.filterFunctionValue = county
            tables = self.get_tableau_view()
            results[county] = [
                tables.get(key) for key in ["Cty In Progress", "Cty All Doses"]
            ]
        return results

    def normalize(self, data: pd.DataFrame) -> pd.DataFrame:
        # get init / complete table
        df = pd.concat([d[0] for d in data.values()])
        # get all doses table
        doses = pd.concat([d[1] for d in data.values()])[
            ["SUM(Administrations)-alias", "AGG(County People Count Label)-alias"]
        ]
        # append total_doses column to main df via join
        df = pd.merge(df, doses, on=self.location_name_col, how="left")

        df["location_name"] = df[self.location_name_col].str.title()
        # parse out data columns

        value_cols = list(set(df.columns) & set(self.cmus.keys()))
        assert len(value_cols) == len(self.cmus)

        out = (
            df.melt(id_vars=["location_name"], value_vars=value_cols)
            .dropna()
            .assign(
                dt=self._retrieve_dt(self.timezone),
                vintage=self._retrieve_vintage(),
                value=lambda x: pd.to_numeric(
                    x["value"].astype(str).str.replace(",", ""),
                ),
            )
            .pipe(self.extract_CMU, cmu=self.cmus)
            .drop(["variable"], axis=1)
        )
        out["location_name"] = out["location_name"].str.replace("In ", "")
        return out
