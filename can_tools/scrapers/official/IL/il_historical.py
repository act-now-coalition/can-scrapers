import pandas as pd
import requests
import us

from can_tools.scrapers.base import ScraperVariable, DatasetBaseNoDate
from can_tools.scrapers.official.base import StateDashboard


class IllinoisHistorical(DatasetBaseNoDate, StateDashboard):
    has_location = False
    source = "https://www.dph.illinois.gov/covid19/covid19-statistics"
    source_name = "Illinois Department of Public Health"
    state_fips = int(us.states.lookup("Illinois").fips)

    def _get_js(self, url: str) -> dict:
        res = requests.get(url)
        if not res.ok:
            raise ValueError("Could not request data from {url}".format(url))

        return res.json()

    def get(self) -> pd.DataFrame:
        url = "https://www.dph.illinois.gov/sitefiles/COVIDHistoricalTestResults.json?nocache=1"
        js = self._get_js(url)
        cats = {
            "confirmed_cases": ScraperVariable(
                category="cases", measurement="cumulative", unit="people"
            ),
            "deaths": ScraperVariable(
                category="deaths",
                measurement="cumulative",
                unit="people",
            ),
            "total_tested": ScraperVariable(
                category="antigen_pcr_tests_total",
                measurement="cumulative",
                unit="unknown",
            ),
        }

        to_cat = []
        for date_data in js["historical_county"]["values"]:
            dt = pd.to_datetime(date_data["testDate"])
            to_cat.append(
                pd.DataFrame(date_data["values"])
                .rename(columns={"County": "county"})
                .melt(id_vars=["county"], value_vars=list(cats.keys()))
                .pipe(self.extract_scraper_variables, cats)
                .assign(dt=dt, vintage=self._retrieve_vintage())
                .drop(["variable"], axis=1)
            )

        unique_cols = [
            "vintage",
            "dt",
            "county",
            "category",
            "unit",
            "measurement",
            "age",
            "race",
            "sex",
        ]

        return pd.concat(to_cat, ignore_index=True, sort=True).drop_duplicates(
            subset=unique_cols
        )
