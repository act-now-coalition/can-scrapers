import pandas as pd

from can_tools.scrapers.base import CMU
from can_tools.scrapers.official.base import FederalDashboard


class CovidTrackingProject(FederalDashboard):
    provider: str = "ctp"
    location_type: str = "state"
    has_location: bool = True
    source: str = "https://api.covidtracking.com/v1/states/daily.csv"

    def fetch(self):
        return pd.read_csv(self.source, parse_dates=["date"])

    def normalize(self, data: pd.DataFrame) -> pd.DataFrame:
        column_map = dict(
            death=CMU(
                category="deaths",
                measurement="cumulative",
                unit="people",
            ),
            hospitalizedCurrently=CMU(
                category="hospital_beds_in_use_covid",
                measurement="current",
                unit="beds",
            ),
            inIcuCurrently=CMU(
                category="icu_beds_in_use_covid",
                measurement="current",
                unit="beds",
            ),
            negative=CMU(
                category="pcr_tests_negative",
                measurement="cumulative",
                unit="unique_people",
            ),
            negativeTestsAntibody=CMU(
                category="antibody_tests_negative",
                measurement="cumulative",
                unit="specimens",
            ),
            negativeTestsPeopleAntibody=CMU(
                category="antibody_tests_negative",
                measurement="cumulative",
                unit="unique_people",
            ),
            negativeTestsViral=CMU(
                category="pcr_tests_negative",
                measurement="cumulative",
                unit="specimens",
            ),
            positive=CMU(
                category="cases",
                measurement="cumulative",
                unit="people",
            ),
            positiveCasesViral=CMU(
                category="pcr_tests_positive",
                measurement="cumulative",
                unit="unique_people",
            ),
            positiveTestsAntibody=CMU(
                category="antibody_tests_positive",
                measurement="cumulative",
                unit="specimens",
            ),
            positiveTestsAntigen=CMU(
                category="antigen_tests_positive",
                measurement="cumulative",
                unit="specimens",
            ),
            positiveTestsPeopleAntibody=CMU(
                category="antibody_tests_positive",
                measurement="cumulative",
                unit="unique_people",
            ),
            positiveTestsPeopleAntigen=CMU(
                category="antigen_tests_positive",
                measurement="cumulative",
                unit="unique_people",
            ),
            positiveTestsViral=CMU(
                category="pcr_tests_positive",
                measurement="cumulative",
                unit="specimens",
            ),
            totalTestsAntigen=CMU(
                category="antigen_tests_total",
                measurement="cumulative",
                unit="specimens",
            ),
            totalTestsAntibody=CMU(
                category="antibody_tests_total",
                measurement="cumulative",
                unit="specimens",
            ),
            totalTestsPeopleAntibody=CMU(
                category="antibody_tests_total",
                measurement="cumulative",
                unit="unique_people",
            ),
            totalTestsPeopleAntigen=CMU(
                category="antigen_tests_total",
                measurement="cumulative",
                unit="unique_people",
            ),
            totalTestsPeopleViral=CMU(
                category="pcr_tests_total",
                measurement="cumulative",
                unit="unique_people",
            ),
            totalTestsViral=CMU(
                category="pcr_tests_total",
                measurement="cumulative",
                unit="specimens",
            ),
        )

        df = (
            data.rename(columns=dict(fips="location", date="dt"))
            .melt(
                id_vars=["dt", "location"],
                value_vars=column_map.keys(),
            )
            .dropna()
            .pipe(self.extract_CMU, column_map)
            .assign(location_type="state", vintage=self._retrieve_vintage())
        )

        return df
