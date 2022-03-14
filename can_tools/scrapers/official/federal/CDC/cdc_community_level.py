import pandas as pd
from can_tools.scrapers import CMU
from can_tools.scrapers.official.base import FederalDashboard


COMMUNITY_LEVEL_MAP = {"Low": 0, "Medium": 1, "High": 2}


class CDCCommunityLevelMetrics(FederalDashboard):
    """Scraper to collect CDC County Community Level and the metrics that determine it.

    Data is updated weekly on Thursdays, and is only available at the county level.
    There is no historical data from before February, 2022, and there does not appear to be a
    secondary dataset containing the historical data (as there is with the testing datasets).
    """

    has_location = True
    location_type = "county"
    source = "https://data.cdc.gov/Public-Health-Surveillance/United-States-COVID-19-Community-Levels-by-County-/3nnm-4jni"
    source_name = "Centers for Disease Control and Prevention"
    provider = "cdc_community_level"
    csv_url = "https://data.cdc.gov/api/views/3nnm-4jni/rows.csv?accessType=DOWNLOAD"

    variables = {
        "covid_inpatient_bed_utilization": CMU(
            category="hospital_beds_in_use_covid",
            measurement="rolling_average_7_day",
            unit="percentage",
        ),
        "covid_hospital_admissions_per_100k": CMU(
            category="covid_hospital_admissions",
            measurement="rolling_average_7_day",
            unit="people_per_100k",
        ),
        "covid_cases_per_100k": CMU(
            category="cases", measurement="rolling_average_7_day", unit="cases_per_100k"
        ),
        "covid-19_community_level": CMU(
            category="cdc_community", measurement="current", unit="risk_level"
        ),
    }

    def fetch(self):
        # Ignore the first row containing no data
        return pd.read_csv(self.csv_url)

    def normalize(self, data: pd.DataFrame) -> pd.DataFrame:

        # Transform percentages to ratios (e.g 3% -> 0.03)
        data["covid_inpatient_bed_utilization"] = (
            pd.to_numeric(data["covid_inpatient_bed_utilization"].str.replace("%", ""))
            / 100
        )

        # Map community levels to integer values where 0 = lowest
        data["covid-19_community_level"].replace(COMMUNITY_LEVEL_MAP, inplace=True)

        return (
            self._rename_or_add_date_and_location(
                data,
                location_column="county_fips",
                date_column="date_updated",
                # Remove state-equivalent territory locations
                locations_to_drop=[
                    "000AS",
                    "000GU",
                    "000VI",
                    "000MP",
                    "UNK",
                    "60000",
                    "66000",
                    "69000",
                    "78000",
                ],
            )
            .pipe(self._reshape_variables, variable_map=self.variables)
            .assign(
                vintage=self._retrieve_vintage(),
            )
        )
