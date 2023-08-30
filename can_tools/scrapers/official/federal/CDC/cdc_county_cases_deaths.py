from pathlib import Path
from typing import List
import pandas as pd
from can_tools.scrapers.official.base import FederalDashboard, CacheMixin
from can_tools.scrapers import variables
from multiprocessing import get_context
import requests
import logging

from can_tools.scrapers.base import ALL_STATES_PLUS_TERRITORIES


class CDCCountyCasesDeathsCacheMixin(CacheMixin):

    cache_dir: Path = Path(__file__).parents[3]

    def check_if_new_data_and_update(self):
        res = requests.get(
            self.cache_url.format(county_fips="06037"),
            timeout=60*60
            )
        data = res.json()
        runid = str(data["runid"])
        cached_runid = self._read_cache()

        # if the runid is the same, then we don't need to update
        if cached_runid == runid:
            return False

        self._write_cache(runid)
        return True


class CDCCountyCasesDeaths(FederalDashboard, CDCCountyCasesDeathsCacheMixin):
    has_location = True
    location_type = "county"
    source = (
        "https://www.cdc.gov/coronavirus/2019-ncov/your-health/covid-by-county.html"
    )
    source_name = "Centers for Disease Control and Prevention"
    provider = "cdc"
    fetch_url = "https://covid.cdc.gov/covid-data-tracker/COVIDData/getAjaxData?id=integrated_county_timeseries_fips_{county_fips}_external"

    variables = {
        "cumulative_cases": variables.CUMULATIVE_CASES_PEOPLE,
        "cumulative_deaths": variables.CUMULATIVE_DEATHS_PEOPLE,
    }

    def __init__(self, execution_dt: pd.Timestamp = pd.Timestamp.utcnow()):
        CDCCountyCasesDeathsCacheMixin.initialize_cache(
            self, cache_url=self.fetch_url, cache_file="cdc_county_cases_deaths.txt"
        )
        super().__init__(execution_dt=execution_dt)

    def fetch(self):
        county_fips: List[str] = [
            self._retrieve_counties(state=state, fips=True)
            for state in ALL_STATES_PLUS_TERRITORIES
        ]
        county_fips = [item.zfill(5) for sublist in county_fips for item in sublist]
        with get_context("spawn").Pool() as pool:
            data = pool.map(self._fetch_county, county_fips)
        return pd.concat(data, axis=0)

    def normalize(self, data: pd.DataFrame) -> pd.DataFrame:
        # Calculate cumulative cases and deaths from new counts.
        data = data.sort_values("date")
        # TODO: Not sure the best way to handle suppressed values. For now, just replace with 0.
        data["deaths_7_day_count_change"] = pd.to_numeric(
            data["deaths_7_day_count_change"].replace({"suppressed": "0"})
        )
        grouping = data.groupby("fips_code")
        data["cumulative_cases"] = grouping["cases_7_day_count_change"].cumsum()
        data["cumulative_deaths"] = grouping["deaths_7_day_count_change"].cumsum()

        return self._rename_or_add_date_and_location(
            data,
            location_column="fips_code",
            date_column="date",
        ).pipe(self._reshape_variables, self.variables, drop_duplicates=True)

    def _fetch_county(self, county_fips):
        url = self.fetch_url.format(county_fips=county_fips)
        try:
            data = requests.get(url).json()
        except requests.exceptions.JSONDecodeError:
            logging.warn("Failed to fetch %s...", county_fips)
            return pd.DataFrame()
        return pd.DataFrame(data["integrated_county_timeseries_external_data"])
