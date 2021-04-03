import textwrap
import pandas as pd
import requests

from can_tools.scrapers.base import CMU, RequestError
from can_tools.scrapers.official.base import FederalDashboard
from can_tools.scrapers import variables


class CDCCountyVaccine(FederalDashboard):
    has_location = True
    location_type = "county"
    source = "https://covid.cdc.gov/covid-data-tracker/#county-view"
    url = (
        "https://covid.cdc.gov/covid-data-tracker/COVIDData/"
        "getAjaxData?id=vaccination_county_condensed_data"
    )
    source_name = "Centers for Disease Control and Prevention"
    provider = "cdc"

    variables = {
        "Series_Complete_Yes": variables.FULLY_VACCINATED_ALL,
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
        "Series_Complete_Pop_Pct": variables.PERCENTAGE_PEOPLE_COMPLETING_VACCINE,
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

    def fetch(self):
        response = requests.get(self.url)
        if not response.ok:
            msg = f"Failed to make request to {self.url}\n"
            msg += "Response from request was\n:"
            msg += textwrap.indent(response.content, "\t")
            raise RequestError(msg)
        return response.json()

    def normalize(self, data):
        df = pd.DataFrame.from_records(data["vaccination_county_condensed_data"])
        out = self._rename_or_add_date_and_location(
            df, location_column="FIPS", date_column="Date", locations_to_drop=["UNK"]
        )
        return self._reshape_variables(out, self.variables)
