import pandas as pd
from can_tools.scrapers import variables, CMU
from can_tools.scrapers.official.base import FederalDashboard


COMMUNITY_LEVEL_MAP = {"Low": 0, "Medium": 1, "High": 2}


class CDCCommunityLevelData(FederalDashboard):
    has_location = True
    location_type = "county"
    source = (
        "https://www.cdc.gov/coronavirus/2019-ncov/your-health/covid-by-county.html"
    )
    source_name = "Centers for Disease Control and Prevention"
    provider = "cdc_community_level"
    csv_url = "https://www.cdc.gov/coronavirus/2019-ncov/json/covid-community-level-structure-2.csv"

    variables = {
        "COVID-19 Community Level - COVID Inpatient Bed Utilization": CMU(
            category="hospital_beds_in_use_covid",
            measurement="rolling_average_7_day",
            unit="percentage",
        ),
        "COVID-19 Community Level - COVID Hospital Admissions per 100k": CMU(
            category="hospital_admissions",
            measurement="rolling_average_7_day",
            unit="people",
        ),
        "COVID-19 Community Level - Cases per 100k": CMU(
            category="cases", measurement="rolling_average_7_day", unit="people"
        ),
        # TODO (sean): This is a not-so-great forced extension of our CMU schema.
        # If we want to scrape more risk levels in the future, it is probably worth
        # revisiting this.
        "COVID-19 Community Level": CMU(
            category="cdc_community", measurement="current", unit="risk_level"
        ),
    }

    def fetch(self):
        # Ignore the first row containing no data
        return pd.read_csv(self.csv_url, skiprows=[0], header=[0])

    def normalize(self, data: pd.DataFrame) -> pd.DataFrame:

        # Transform percentages to ratios (e.g 3% -> 0.03)
        data["COVID-19 Community Level - COVID Inpatient Bed Utilization"] = (
            pd.to_numeric(
                data[
                    "COVID-19 Community Level - COVID Inpatient Bed Utilization"
                ].str.replace("%", "")
            )
            / 100
        )

        # Map community levels to integer values where 0 = lowest
        data["COVID-19 Community Level"].replace(COMMUNITY_LEVEL_MAP, inplace=True)

        return (
            self._rename_or_add_date_and_location(
                data,
                location_column="FIPS",
                timezone="US/Eastern",
                locations_to_drop=["UNK"],
            )
            .pipe(self._reshape_variables, variable_map=self.variables)
            .assign(
                dt=self._retrieve_dt("US/Eastern"),  # using EST as default
                vintage=self._retrieve_vintage(),
            )
        )
