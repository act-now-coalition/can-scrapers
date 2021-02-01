import pandas as pd
import requests
import us

from can_tools.scrapers.base import CMU
from can_tools.scrapers.official.base import TableauDashboard


class TennesseeVaccineCountyFirstDose(TableauDashboard):
    has_location = False
    source = "https://www.tn.gov/health/cedep/ncov/covid-19-vaccine-information.html"
    state_fips = int(us.states.lookup("Tennessee").fips)
    location_type = "county"
    baseurl = "https://data.tn.gov/t/Public"
    viewPath = "TennIISCOVID-19VaccineReporting/1STDOSE"
    dataset_key: str = r"% PART VACC MAP"
    value_column: str = r"only one dose %"
    category: str = "total_vaccine_initiated"

    def fetch(self) -> pd.DataFrame:
        return self.get_tableau_view()[self.dataset_key]

    def normalize(self, data: pd.DataFrame) -> pd.DataFrame:
        val_col = [k for k in list(data) if self.value_column in k.lower()]
        assert len(val_col) == 1

        county_col = [k for k in list(data) if "patient county-value" in k.lower()]
        assert len(county_col) == 1

        variable_colname = "vaccine_pct"
        cmus = {
            variable_colname: CMU(
                category=self.category,
                measurement="current",
                unit="percentage",
            )
        }
        odd_cap_counties = {
            "Mcnairy": "McNairy",
            "Mcminn": "McMinn",
            "Dekalb": "DeKalb",
        }
        df = (
            data[val_col + county_col]
            .rename(columns={val_col[0]: "value", county_col[0]: "location_name"})
            .assign(
                location_name=lambda x: x["location_name"]
                .str.title()
                .replace(odd_cap_counties),
                variable=variable_colname,
                dt=self._retrieve_dt(tz="US/Central"),
                vintage=self._retrieve_vintage(),
            )
            .pipe(self.extract_CMU, cmu=cmus)
            .drop(["variable"], axis="columns")
        )
        return df


class TennesseeVaccineCountySecondDose(TennesseeVaccineCountyFirstDose):
    viewPath = "TennIISCOVID-19VaccineReporting/2NDDOSE"
    dataset_key: str = r"% FULL VACC MAP"
    value_column: str = r"two doses %"
    category: str = "total_vaccine_completed"
