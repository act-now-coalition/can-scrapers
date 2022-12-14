import pandas as pd
from numpy import nan
from typing import List, Tuple, Dict
from can_tools.scrapers.official.base import FederalDashboard, ETagCacheMixin

from can_tools.scrapers.variables import (
    CUMULATIVE_CASES_PEOPLE,
    CUMULATIVE_DEATHS_PEOPLE,
)

NYTIMES_RAW_BASE_URL = "https://raw.githubusercontent.com/nytimes/covid-19-data/master/"

COUNTY_BACKFILLED_CASES = [
    # https://www.collincountytx.gov/public_information/news/Pages/08142020_COVIDreport.aspx
    # Collin county had a large backfill of 1175 cases.  It's not known how many of these cases
    # are backfill vs new cases. Assuming that cases are around the previous and next day
    # (132 and 395) subtracting 900 cases to give 275 new cases for that day.
    ("48085", "2020-08-14", 900),
    # https://www.dallasnews.com/news/public-health/2020/08/16/backlog-in-state-reporting-adds-more-than-5000-coronavirus-cases-in-dallas-county/
    # Of the 5,361 cases reported Sunday, 5,195 came from the backlog, according to a news release from Dallas County Judge Clay Jenkins.
    ("48113", "2020-08-16", 5195),
    # https://www.dallascounty.org/Assets/uploads/docs/covid-19/press-releases/august/081720-PressRelease-DallasCountyReports1850AdditionalPositiveCOVID-19Cases.pdf
    # Dallas County has requested the breakdown of backfilled/non-backfilled cases
    # but has not yet received it: https://twitter.com/JudgeClayJ/status/1295836564887678976?s=20
    # 1500 is an estimate that puts daily increases inline with other days (leaving around 300 new cases).
    ("48113", "2020-08-17", 1500),
    # https://www.dallasnews.com/news/public-health/2020/08/18/dallas-county-expects-to-report-about-550-backlogged-coronavirus-cases-237-new-cases/
    ("48113", "2020-08-18", 550),
    # https://www.dallascounty.org/Assets/uploads/docs/covid-19/press-releases/august/082020-PressRelease-DallasCountyReports308AdditionalPositiveCOVID-19Cases.pdf
    ("48113", "2020-08-19", 206),
    # https://www.dallascounty.org/Assets/uploads/docs/covid-19/press-releases/august/082120-PressRelease-DallasCountyReports714AdditionalPositiveCOVID-19Cases.pdf
    ("48113", "2020-08-21", 459),
    # https://www.dallascounty.org/Assets/uploads/docs/covid-19/press-releases/august/082220-PressRelease-DallasCountyReports1086AdditionalPositiveCOVID-19Cases.pdf
    ("48113", "2020-08-22", 862),
    # https://www.dallascounty.org/Assets/uploads/docs/covid-19/press-releases/august/082320-PressRelease-DallasCountyReports1086AdditionalPositiveCOVID-19Cases.pdf
    ("48113", "2020-08-23", 93),
    # https://www.dallascounty.org/Assets/uploads/docs/covid-19/press-releases/august/082420-PressRelease-DallasCountyReports182AdditionalPositiveCOVID-19Cases.pdf
    ("48113", "2020-08-24", 84),
    # https://covid-19-in-jefferson-county-ky-lojic.hub.arcgis.com/
    ("21111", "2020-09-03", 500),
    # https://www.ibj.com/articles/official-covid-19-cases-surpass-20000-in-marion-county/
    # NYT inaccurately showed 411 instead of 426
    ("18097", "2020-09-13", 396),
    # https://www.ksat.com/news/local/2020/09/14/142-new-covid-19-cases-24-deaths-reported-for-san-antonio-bexar-county/
    ("48029", "2020-09-13", 1575),
    # https://harriscounty.maps.arcgis.com/apps/opsdashboard/index.html#/c0de71f8ea484b85bb5efcb7c07c6914
    ("48201", "2020-09-15", 2378),
    # https://trello.com/c/CEkDc3ZO/462-large-tx-backfill-on-9-21
    ("48029", "2020-09-20", 2078),  # Bexar County, TX
    ("48113", "2020-09-20", 306),  # Dallas County, TX
    ("48001", "2020-09-21", 1051),  # Anderson County, TX
    ("48085", "2020-09-21", 3),  # Collin County, TX
    ("48163", "2020-09-21", 298),  # Frio County, TX
    ("48201", "2020-09-21", 328),  # Harris County, TX
    ("48225", "2020-09-21", 1),  # Houston County, TX
    ("48439", "2020-09-21", 125),  # Tarrant County, TX
    ("48013", "2020-09-21", 522),  # Atascosa County, TX
    ("48019", "2020-09-21", 41),  # Bandera County, TX
    ("48057", "2020-09-21", 186),  # Calhoun County, TX
    ("48113", "2020-09-21", 2),  # Dallas County, TX
    ("48127", "2020-09-21", 53),  # Dimmit County, TX
    ("48137", "2020-09-21", 33),  # Edwards County, TX
    ("48171", "2020-09-21", 96),  # Gillespie County, TX
    ("48177", "2020-09-21", 234),  # Gonzales County, TX
    ("48187", "2020-09-21", 1587),  # Guadalupe County, TX
    ("48201", "2020-09-21", 13622),  # Harris County, TX
    ("48239", "2020-09-21", 77),  # Jackson County, TX
    ("48255", "2020-09-21", 181),  # Karnes County, TX
    ("48265", "2020-09-21", 142),  # Kerr County, TX
    ("48271", "2020-09-21", 19),  # Kinney County, TX
    ("48285", "2020-09-21", 252),  # Lavaca County, TX
    ("48355", "2020-09-21", 231),  # Nueces County, TX
    ("48385", "2020-09-21", 12),  # Real County, TX
    ("48493", "2020-09-21", 307),  # Wilson County, TX
    ("48507", "2020-09-21", 52),  # Zavala County, TX
    ("48407", "2020-09-22", 1),  # San Jacinto, TX
    # https://trello.com/c/GKUHUbyK/483-2020-10-03-collin-county-tx-spike-on-daily-new-cases
    ("48201", "2020-10-02", 2438),  # Harris County, TX
    ("48085", "2020-10-03", 1202),  # Collin County, TX
]

STATE_BACKFILLED_CASES = [
    # On 2020-07-24, CT reported a backfill of 440 additional positive cases.
    # https://portal.ct.gov/Office-of-the-Governor/News/Press-Releases/2020/07-2020/Governor-Lamont-Coronavirus-Update-July-24
    ("09", "2020-07-24", 440),
    # https://portal.ct.gov/Office-of-the-Governor/News/Press-Releases/2020/07-2020/Governor-Lamont-Coronavirus-Update-July-29
    ("09", "2020-07-29", 384),
    # http://www.floridahealth.gov/newsroom/2020/09/090120-1112-covid19.pr.html
    ("12", "2020-09-01", 3870),
    # https://twitter.com/DHSWI/status/1301980028209713153?s=20
    # This number is a bit fuzzy, but on 9/3 there were 767 cases.  There were 1400 cases
    # on 9/4 including backfill.  Estimating 800 cases on 9/4.
    ("55", "2020-09-04", 1400 - 800),
    # https://directorsblog.health.azdhs.gov/covid-19-antigen-tests/
    ("04", "2020-09-17", 577),
    # https://twitter.com/AZDHS/status/1306974258124468226
    ("04", "2020-09-18", 764),
    # https://trello.com/c/9EbBRV1Z/472-2020-09-26-north-carolina-change-in-methodology-to-count-cases
    ("37", "2020-09-25", 4544),
    # https://www.al.com/news/2020/10/alabama-adds-3852-covid-cases-after-influx-of-backlogged-data-dating-back-to-june.html
    ("01", "2020-10-23", 2565),
]


class NYTimesCasesDeaths(FederalDashboard, ETagCacheMixin):
    source_name = "The New York Times"
    source = "https://github.com/nytimes/covid-19-data"

    has_location = True
    provider: str = "nyt"
    location_type = ""  # multiple location types, so we identify the type with a column
    file_slugs = {
        "county": [
            "us-counties-2020.csv",
            "us-counties-2021.csv",
            "us-counties-2022.csv",
        ],
        "state": ["us-states.csv"],
        "nation": ["us.csv"],
    }

    variables = {
        "cases": CUMULATIVE_CASES_PEOPLE,
        "deaths": CUMULATIVE_DEATHS_PEOPLE,
    }

    def __init__(self, execution_dt: pd.Timestamp = pd.Timestamp.utcnow()):
        # NOTE (2022/12/14): We used to use https://api.github.com/repos/nytimes/covid-19-data/commits for the cache_url,
        # so that it would pick up any changes to the repo, but NYT makes several commits per day to files we don't need, so
        # for now we just use https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-states.csv and hope that
        # it always updates when us-counties updates.
        ETagCacheMixin.initialize_cache(
            self,
            cache_url="https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-states.csv",
            cache_file="nyt_cases_deaths.txt",
        )
        super().__init__(execution_dt=execution_dt)

    def fetch(self) -> pd.DataFrame:
        data = []
        for location_type, files in self.file_slugs.items():
            for file in files:
                location = pd.read_csv(
                    NYTIMES_RAW_BASE_URL + file, dtype={"fips": str}
                ).assign(location_type=location_type)

                # Nation-level file does not have FIPS column, so create one
                if file in ["us.csv"]:
                    location = location.assign(fips=0)
                data.append(location)
        return pd.concat(data)

    def normalize(self, data: pd.DataFrame) -> pd.DataFrame:
        data = remove_county_backfilled_cases(data, COUNTY_BACKFILLED_CASES)
        data = remove_state_backfilled_cases(data, STATE_BACKFILLED_CASES)
        data = remove_ma_county_zeroes_data(data)
        return (
            # Remove records with no FIPS codes
            data.replace("nan", nan)
            .loc[~data["fips"].isna(), :]
            .pipe(
                self._rename_or_add_date_and_location,
                location_column="fips",
                date_column="date",
            )
            .assign(location=lambda row: pd.to_numeric(row["location"]))
            .pipe(
                self._reshape_variables,
                variable_map=self.variables,
                id_vars=["location_type"],
            )
            # two Alaska FIPS not in our location csv
            # The location 78020 (St. John Island, VI) caused problems in the pipeline.
            # It was being assigned a location id "iso1:us#iso2:us-vi#fips:780120" (instead of "iso1:us#iso2:us-vi#fips:78020" with the correct FIPS)
            # which caused the update-and-promote-datasets to fail. Since we do not surface any data for VI, we just block the location entirely.
            .query("location not in [2997, 2998, 78020, 48999]")
        )


def remove_county_backfilled_cases(
    data: pd.DataFrame, backfilled_cases: List[Tuple[str, str, int]]
) -> pd.DataFrame:
    """Removes reported county backfilled cases from case totals.
    Args:
        data: Data
        backfilled_cases: List of backfilled case info.
    Returns: Updated data frame.
    """
    for county_fips, date, cases in backfilled_cases:
        is_on_or_after_date = data["date"] >= date
        is_fips_data_after_date = is_on_or_after_date & (data["fips"] == county_fips)
        data.loc[is_fips_data_after_date, "cases"] -= cases

        # Remove county count from state counts as well
        state_fips = county_fips[:2]
        is_fips_data_after_date = is_on_or_after_date & (data["fips"] == state_fips)
        if is_fips_data_after_date.any():
            data.loc[is_fips_data_after_date, "cases"] -= cases

    return data


def remove_state_backfilled_cases(
    data: pd.DataFrame, backfilled_cases: List[Tuple[str, str, int]]
) -> pd.DataFrame:
    """Removes reported backfilled cases from case totals.
    Args:
        data: Data
        backfilled_cases: List of backfilled case info.
    Returns: Updated data frame.
    """
    for state_fips, date, cases in backfilled_cases:
        adjustments = _calculate_county_adjustments(data, date, cases, state_fips)
        is_on_or_after_date = data["date"] >= date
        for fips, count in adjustments.items():
            is_fips_data_after_date = is_on_or_after_date & (data["fips"] == fips)
            data.loc[is_fips_data_after_date, "cases"] -= int(count)

        # Remove state counts also.
        is_fips_data_after_date = is_on_or_after_date & (data["fips"] == state_fips)
        if is_fips_data_after_date.any():
            data.loc[is_fips_data_after_date, "cases"] -= cases

    return data


def _calculate_county_adjustments(
    data: pd.DataFrame, date: str, backfilled_cases: int, state_fips: str
) -> Dict[str, int]:
    """Calculating number of cases to remove per county, weighted on number of new cases per county.
    Weighting on number of new cases per county gives a reasonable measure of where the backfilled
    cases ended up.
    Args:
        data: Input Data.
        date: Date of backfill.
        backfilled_cases: Number of backfilled cases.
        state_fips: FIPS code for state.
    Returns: Dictionary of estimated fips -> backfilled cases.
    """
    is_state = data["fips"].str.match(f"{state_fips}[0-9][0-9][0-9]")
    is_not_unknown = data["fips"] != f"{state_fips}999"
    if not (is_not_unknown & is_state).any():
        return {}

    fields = ["date", "fips", "cases"]
    cases = (
        data.loc[is_state & is_not_unknown, fields]
        .set_index(["fips", "date"])
        .sort_index()
    )
    cases = cases.diff().reset_index(level=1)
    cases_on_date = cases[cases.date == date]["cases"]
    # For states with more counties, rounding could lead to the sum of the counties diverging from
    # the backfilled cases count.
    return (cases_on_date / cases_on_date.sum() * backfilled_cases).round().to_dict()


def remove_ma_county_zeroes_data(
    data: pd.DataFrame,
    county_reporting_stopped_date="2020-08-11",
    county_reporting_restart_date="2020-08-18",
):
    """Removes county data for mass where cases are not increasing due to data reporting change.
    Massachussetts stopped reporting county case data after 8/11.  This code removes data for those
    days, treating the data as missing rather than a count of 0.
    Args:
        data: Data to clean up.
        county_reporting_stopped_date: Date to start checking for no case count increases.
        county_reporting_restart_date: Date that MA county reporting started up again.
    Returns: Data with Mass county data properly cleaned up.
    """
    # Sorting on fips and date to ensure the diff is applied in ascending date order below.
    data = data.sort_values(["fips", "date"])

    is_county = data["location_type"] == "county"
    is_ma = data["state"] == "MA"
    is_during_reporting_lull = data["date"].between(
        county_reporting_stopped_date, county_reporting_restart_date
    )
    is_ma_county_after_reporting = is_county & is_ma & is_during_reporting_lull
    ma_county_data = data.loc[is_ma_county_after_reporting]
    cases_to_remove = ma_county_data.groupby("fips")["cases"].diff() == 0
    return pd.concat(
        [data.loc[~is_ma_county_after_reporting], ma_county_data.loc[~cases_to_remove]]
    )
