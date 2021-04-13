import pandas as pd
import requests
import us

from can_tools.scrapers.base import CMU
from can_tools.scrapers.official.base import TableauDashboard


class TennesseeVaccineCounty(TableauDashboard):
    has_location = False
    source = "https://www.tn.gov/health/cedep/ncov/covid-19-vaccine-information.html"
    source_name = "Tennessee Department of Health"
    state_fips = int(us.states.lookup("Tennessee").fips)
    location_type = "county"
    baseurl = "https://data.tn.gov/t/Public"
    viewPath = "TennIISCOVID-19VaccineReporting/SUMMARY"
    category: str = "total_vaccine_initiated"

    data_tableau_table = "REPORTING"
    location_name_col = "Patient County-alias"
    timezone = "US/Central"

    cmus = {
        "SUM(% At Least One % (copy))-alias": CMU(
            category="total_vaccine_initiated",
            measurement="current",
            unit="percentage",
        ),
        "SUM(% Two Doses %)-alias": CMU(
            category="total_vaccine_completed",
            measurement="current",
            unit="percentage",
        ),
    }

    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        df = super().normalize(df)
        odd_cap_counties = {
            "Mcnairy": "McNairy",
            "Mcminn": "McMinn",
            "Dekalb": "DeKalb",
        }
        df["location_name"] = df["location_name"].str.title().replace(odd_cap_counties)
        return df
