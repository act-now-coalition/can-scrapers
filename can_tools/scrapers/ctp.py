import pandas as pd
import us

from can_tools.scrapers.base import ScraperVariable
from can_tools.scrapers.official.base import FederalDashboard


def _find_fips(abbr):
    return int(us.states.lookup(abbr).fips)


class CovidTrackingProjectDemographics(FederalDashboard):
    provider: str = "ctp"
    location_type: str = "state"
    has_location: bool = True
    source: str = "https://covidtracking.com/race/dashboard"
    source_name = "The COVID Tracking Project"

    def fetch(self):
        url = (
            "https://docs.google.com/spreadsheets/d/e/"
            "2PACX-1vS8SzaERcKJOD_EzrtCDK1dX1zkoMochlA9iHoHg_RSw3V8bkpfk1mpw4pfL5RdtSOyx_oScsUtyXyk"
            "/pub?gid=43720681&single=true&output=csv"
        )
        return pd.read_csv(url)

    def normalize(self, data: pd.DataFrame) -> pd.DataFrame:
        def _cases(race, ethnicity="all"):
            return ScraperVariable(
                category="cases",
                measurement="cumulative",
                unit="people",
                race=race,
                ethnicity=ethnicity,
            )

        def _deaths(race, ethnicity="all"):
            return ScraperVariable(
                category="deaths",
                measurement="cumulative",
                unit="people",
                race=race,
                ethnicity=ethnicity,
            )

        def _hosp(race, ethnicity="all"):
            return ScraperVariable(
                category="hospital_beds_in_use_covid",
                measurement="current",
                unit="beds",
                race=race,
                ethnicity=ethnicity,
            )

        column_map = {
            "Cases_Total": _cases("all"),
            "Cases_White": _cases("white"),
            "Cases_Black": _cases("black"),
            "Cases_Latinx": _cases("latinx"),
            "Cases_Asian": _cases("asian"),
            "Cases_AIAN": _cases("ai_an"),
            "Cases_NHPI": _cases("pacific_islander"),
            "Cases_Multiracial": _cases("multiple_other"),
            "Cases_Other": _cases("other"),
            "Cases_Unknown": _cases("unknown"),
            "Cases_Ethnicity_Hispanic": _cases("all", "hispanic"),
            "Cases_Ethnicity_NonHispanic": _cases("all", "non-hispanic"),
            "Cases_Ethnicity_Unknown": _cases("all", "unknown"),
            "Deaths_Total": _deaths("all"),
            "Deaths_White": _deaths("white"),
            "Deaths_Black": _deaths("black"),
            "Deaths_Latinx": _deaths("latinx"),
            "Deaths_Asian": _deaths("asian"),
            "Deaths_AIAN": _deaths("ai_an"),
            "Deaths_NHPI": _deaths("pacific_islander"),
            "Deaths_Multiracial": _deaths("multiple_other"),
            "Deaths_Other": _deaths("other"),
            "Deaths_Unknown": _deaths("unknown"),
            "Deaths_Ethnicity_Hispanic": _deaths("all", "hispanic"),
            "Deaths_Ethnicity_NonHispanic": _deaths("all", "non-hispanic"),
            "Deaths_Ethnicity_Unknown": _deaths("all", "unknown"),
            "Hosp_Total": _hosp("all"),
            "Hosp_White": _hosp("white"),
            "Hosp_Black": _hosp("black"),
            "Hosp_Latinx": _hosp("latinx"),
            "Hosp_Asian": _hosp("asian"),
            "Hosp_AIAN": _hosp("ai_an"),
            "Hosp_NHPI": _hosp("pacific_islander"),
            "Hosp_Multiracial": _hosp("multiple_other"),
            "Hosp_Other": _hosp("other"),
            "Hosp_Unknown": _hosp("unknown"),
            "Hosp_Ethnicity_Hispanic": _hosp("all", "hispanic"),
            "Hosp_Ethnicity_NonHispanic": _hosp("all", "non-hispanic"),
            "Hosp_Ethnicity_Unknown": _hosp("all", "unknown"),
            # "Tests_Total",
            # "Tests_White",
            # "Tests_Black",
            # "Tests_LatinX",
            # "Tests_Asian",
            # "Tests_AIAN",
            # "Tests_NHPI",
            # "Tests_Multiracial",
            # "Tests_Other",
            # "Tests_Unknown",
            # "Tests_Ethnicity_Hispanic",
            # "Tests_Ethnicity_NonHispanic",
            # "Tests_Ethnicity_Unknown",
        }
        # Fill `#REF!` entries with NaN
        data.Cases_Black = pd.to_numeric(data.Cases_Black, errors="coerce")

        return (
            data.assign(
                location=lambda x: x["State"].map(_find_fips),
                dt=lambda x: pd.to_datetime(x["Date"].astype(str)),
            )
            .melt(
                id_vars=["dt", "location"],
                value_vars=column_map.keys(),
            )
            .dropna()
            .pipe(self.extract_ScraperVariable, column_map)
            .assign(
                location_type="state",
                vintage=self._retrieve_vintage(),
                value=lambda x: pd.to_numeric(
                    x["value"].astype(str).str.replace(",", "")
                ),
            )
            .drop(["variable"], axis=1)
        )


class CovidTrackingProject(FederalDashboard):
    provider: str = "ctp"
    location_type: str = "state"
    has_location: bool = True
    source: str = "https://api.covidtracking.com/v1/states/daily.csv"
    source_name = "The COVID Tracking Project"

    def fetch(self):
        return pd.read_csv(self.source, parse_dates=["date"])

    def normalize(self, data: pd.DataFrame) -> pd.DataFrame:
        column_map = dict(
            death=ScraperVariable(
                category="deaths",
                measurement="cumulative",
                unit="people",
            ),
            hospitalizedCurrently=ScraperVariable(
                category="hospital_beds_in_use_covid",
                measurement="current",
                unit="beds",
            ),
            inIcuCurrently=ScraperVariable(
                category="icu_beds_in_use_covid",
                measurement="current",
                unit="beds",
            ),
            negative=ScraperVariable(
                category="pcr_tests_negative",
                measurement="cumulative",
                unit="unique_people",
            ),
            negativeTestsAntibody=ScraperVariable(
                category="antibody_tests_negative",
                measurement="cumulative",
                unit="specimens",
            ),
            negativeTestsPeopleAntibody=ScraperVariable(
                category="antibody_tests_negative",
                measurement="cumulative",
                unit="unique_people",
            ),
            negativeTestsViral=ScraperVariable(
                category="pcr_tests_negative",
                measurement="cumulative",
                unit="specimens",
            ),
            positive=ScraperVariable(
                category="cases",
                measurement="cumulative",
                unit="people",
            ),
            positiveCasesViral=ScraperVariable(
                category="pcr_tests_positive",
                measurement="cumulative",
                unit="unique_people",
            ),
            positiveTestsAntibody=ScraperVariable(
                category="antibody_tests_positive",
                measurement="cumulative",
                unit="specimens",
            ),
            positiveTestsAntigen=ScraperVariable(
                category="antigen_tests_positive",
                measurement="cumulative",
                unit="specimens",
            ),
            positiveTestsPeopleAntibody=ScraperVariable(
                category="antibody_tests_positive",
                measurement="cumulative",
                unit="unique_people",
            ),
            positiveTestsPeopleAntigen=ScraperVariable(
                category="antigen_tests_positive",
                measurement="cumulative",
                unit="unique_people",
            ),
            positiveTestsViral=ScraperVariable(
                category="pcr_tests_positive",
                measurement="cumulative",
                unit="specimens",
            ),
            totalTestsAntigen=ScraperVariable(
                category="antigen_tests_total",
                measurement="cumulative",
                unit="specimens",
            ),
            totalTestsAntibody=ScraperVariable(
                category="antibody_tests_total",
                measurement="cumulative",
                unit="specimens",
            ),
            totalTestsPeopleAntibody=ScraperVariable(
                category="antibody_tests_total",
                measurement="cumulative",
                unit="unique_people",
            ),
            totalTestsPeopleAntigen=ScraperVariable(
                category="antigen_tests_total",
                measurement="cumulative",
                unit="unique_people",
            ),
            totalTestsPeopleViral=ScraperVariable(
                category="pcr_tests_total",
                measurement="cumulative",
                unit="unique_people",
            ),
            totalTestsViral=ScraperVariable(
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
            .pipe(self.extract_ScraperVariable, column_map)
            .assign(location_type="state", vintage=self._retrieve_vintage())
        )

        return df
