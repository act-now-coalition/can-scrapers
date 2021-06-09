import json
import pandas as pd
import us
from typing import Dict, List
from can_tools.scrapers import variables
from can_tools.scrapers.official.base import ArcGIS, TableauDashboard


class WAKingCountyVaccine(TableauDashboard):
    has_location = True
    source = "https://kingcounty.gov/depts/health/covid-19/data/vaccination.aspx"
    source_name = "Public Health Seattle & King County"
    state_fips = int(us.states.lookup("Washington").fips)
    location_type = "county"
    baseurl = "https://tableaupub.kingcounty.gov"
    viewPath = "COVIDVaccinePublicDashboardV2/FrontPage"
    data_tableau_table = "People table"
    timezone = "US/Pacific"

    url = f"https://tableaupub.kingcounty.gov/t/Public/views/COVIDVaccinePublicDashboardV2/FrontPage?:embed=y&:showVizHome=no&:host_url=https%3A%2F%2Ftableaupub.kingcounty.gov%2F&:embed_code_version=3&:tabs=yes&:toolbar=yes&:showAppBanner=false&:display_spinner=no&:loadOrderID=0"
    # map wide form column names into CMUs
    variables = {
        "Fully vaccinted": variables.FULLY_VACCINATED_ALL,
        "At least 1 dose": variables.INITIATING_VACCINATIONS_ALL,
    }

    def fetch(self) -> pd.DataFrame:
        tables = self.get_tableau_view(url=self.url)
        return tables[self.data_tableau_table]

    def normalize(self, data: pd.DataFrame) -> pd.DataFrame:
        cols = {"SUM(N)-alias": "value", "Measure-value": "variable"}
        return (
            data.rename(columns=cols)
            .loc[:, cols.values()]
            .assign(
                location=53055,
                dt=self._retrieve_dtm1d(self.timezone),
                vintage=self._retrieve_vintage(),
                value=lambda x: pd.to_numeric(x["value"]),
            )
            .pipe(self.extract_CMU, cmu=self.variables)
            .drop(columns={"variable"})
        )


class WAPierceCountyVaccine(ArcGIS):
    ARCGIS_ID = "691gUcz8Lfc8VwnZ"
    has_location = True
    location_type = "county"
    state_fips = int(us.states.lookup("Washington").fips)
    source = "https://www.tpchd.org/healthy-people/diseases/covid-19/covid-vaccine-data"
    source_name = "Tacoma-Pierce County Health Department"
    variables = {
        "vaccine_completed": variables.FULLY_VACCINATED_ALL,
        "vaccine_intiatied": variables.INITIATING_VACCINATIONS_ALL,
    }

    def fetch(self) -> List[Dict]:
        return self.get_all_jsons("Vaccine_Weekly_Metrics", 0, 9)

    def normalize(self, data) -> pd.DataFrame:
        cols = {
            "PCResFullyVacc": "vaccine_completed",
            "AllPCVacc": "vaccine_intiatied",
            "DateReportedThru": "dt",
        }
        return (
            self.arcgis_jsons_to_df(data)
            .rename(columns=cols)
            .loc[:, cols.values()]
            .assign(
                location=53033,
                dt=lambda x: pd.to_datetime(x["dt"], unit="ms").dt.date,
            )
            .pipe(self._reshape_variables, variable_map=self.variables)
        )
