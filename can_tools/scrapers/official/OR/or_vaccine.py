import pandas as pd
import requests
import us

from can_tools.scrapers.base import CMU
from can_tools.scrapers.official.base import TableauDashboard

class OregonVaccineCounty(TableauDashboard):
    has_location = False
    source = "https://covidvaccine.oregon.gov/"
    state_fips = int(us.states.lookup("Oregon").fips)
    location_type = "county"

    baseurl = "https://public.tableau.com/"
    viewPath = "OregonCOVID-19VaccinationTrends/OregonCountyVaccinationTrends"

    def fetch(self) -> pd.DataFrame:
        return self.get_tableau_view()[]

    def normalize(self, data: pd.DataFrame) -> pd.DataFrame:
        value_columns = [r"sum(metric - in progress)-alias", r"sum(metric - total people)-alias", ]
        val_col = [k for k in list(data) if k.lower() in value_columns]
        assert len(val_col) == 2

        dose1_ix1 = "in progress" in val_col[1].lower()
        dose2_ix1 = "total people" in val_col[1].lower()

        county_col = [k for k in list(data) if "recip address county-alias" in k.lower()]
        assert len(county_col) == 1

        col_map = {
            val_col[dose1_ix1]: "first",
            val_col[dose2_ix1]: "second",
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

        df = (
            data[val_col + county_col]
            .rename(columns=col_map)
            .melt(id_vars=["location_name"])
            .assign(
                location_name=lambda x: x["location_name"]
                .str.title(),
                dt=self._retrieve_dt(tz="US/Pacific"),
                vintage=self._retrieve_vintage(),
            )
            .pipe(self.extract_CMU, cmu=cmus)
            .drop(["variable"], axis="columns")
        )
        return df