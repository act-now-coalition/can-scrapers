import pandas as pd
import requests
import us

from can_tools.scrapers.base import CMU
from can_tools.scrapers.official.base import TableauDashboard


class TennesseeVaccineCounty(TableauDashboard):
    has_location = False
    source = "https://www.tn.gov/health/cedep/ncov/covid-19-vaccine-information.html"
    state_fips = int(us.states.lookup("Tennessee").fips)
    location_type = "county"
    baseurl = "https://data.tn.gov/t/Public"
    viewPath = "TennIISCOVID-19VaccineReporting/SUMMARY"
    category: str = "total_vaccine_initiated"

    def fetch(self) -> pd.DataFrame:
        return self.get_tableau_view()["REPORTING"]

    def normalize(self, data: pd.DataFrame) -> pd.DataFrame:
        df = data.copy()

        df.columns = [x.lower() for x in df]
        value_columns = [x for x in df.columns if ("at least one" in x) or ("two doses" in x)]
        val_col = [k for k in list(data) if k.lower() in value_columns]
        assert len(val_col) == 2

        dose1_ix1 = "at least one" in val_col[1].lower()

        county_col = [k for k in list(data) if "patient county-value" in k.lower()]
        assert len(county_col) == 1

        col_map = {
            val_col[dose1_ix1]: "first",
            val_col[~dose1_ix1]: "second",
            county_col[0]: "location_name",
        }

        cmus = {
            "first": CMU(
                category="total_vaccine_initiated",
                measurement="current",
                unit="percentage",
            ),
            "second": CMU(
                category="total_vaccine_completed",
                measurement="current",
                unit="percentage",
            ),
        }
        odd_cap_counties = {
            "Mcnairy": "McNairy",
            "Mcminn": "McMinn",
            "Dekalb": "DeKalb",
        }
        df = (
            data[val_col + county_col]
            .rename(columns=col_map)
            .melt(id_vars=["location_name"])
            .assign(
                location_name=lambda x: x["location_name"]
                .str.title()
                .replace(odd_cap_counties),
                dt=self._retrieve_dt(tz="US/Central"),
                vintage=self._retrieve_vintage(),
            )
            .pipe(self.extract_CMU, cmu=cmus)
            .drop(["variable"], axis="columns")
        )
        return df
