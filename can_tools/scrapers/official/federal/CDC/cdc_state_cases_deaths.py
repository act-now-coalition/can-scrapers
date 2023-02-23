import pandas as pd
import us
from can_tools.scrapers import variables
from can_tools.scrapers.official.base import ETagCacheMixin, FederalDashboard


class CDCStateCasesDeaths(FederalDashboard, ETagCacheMixin):
    has_location = True
    location_type = "state"
    source = "https://data.cdc.gov/Case-Surveillance/Weekly-United-States-COVID-19-Cases-and-Deaths-by-/pwn4-m3yp"
    source_name = "Centers for Disease Control and Prevention"
    provider = "cdc"
    csv_url = "https://data.cdc.gov/api/views/pwn4-m3yp/rows.csv?accessType=DOWNLOAD"

    variables = {
        "tot_cases": variables.CUMULATIVE_CASES_PEOPLE,
        "tot_deaths": variables.CUMULATIVE_DEATHS_PEOPLE,
    }

    # Send URL and filename that Mixin will use to check the etag
    def __init__(self, execution_dt: pd.Timestamp = pd.Timestamp.utcnow()):
        ETagCacheMixin.initialize_cache(
            self, cache_url=self.csv_url, cache_file="cdc_state_cases_deaths.txt"
        )
        super().__init__(execution_dt=execution_dt)

    def fetch(self):
        return pd.read_csv(self.csv_url)

    def normalize(self, data: pd.DataFrame) -> pd.DataFrame:
        data = _combine_ny_and_nyc(data)
        # Filter out territories we don't have fips codes for
        data = data[~data["state"].isin(["FSM", "RMI", "PW"])]
        data["location"] = data["state"].map(
            lambda state_name: int(us.states.lookup(state_name).fips)
        )
        return self._rename_or_add_date_and_location(
            data,
            location_column="fips",
            date_column="end_date",
        ).pipe(self._reshape_variables, self.variables, drop_duplicates=True)


def _combine_ny_and_nyc(data: pd.DataFrame):
    """NYC data is reported separately from NY data. Combine them into one row.

    See "Number of Jurisdictions Reporting" footnote in the data source:
    https://data.cdc.gov/Case-Surveillance/Weekly-United-States-COVID-19-Cases-and-Deaths-by-/pwn4-m3yp
    """
    ny = data[data["state"] == "NY"]
    nyc = data[data["state"] == "NYC"]
    nyc["state"] = "NY"
    combined = pd.concat([ny, nyc], axis=0)
    combined = combined.groupby(["state", "end_date"]).sum().reset_index()

    data = data[(data["state"] != "NY") & (data["state"] != "NYC")]
    return pd.concat([data, combined], axis=0)
