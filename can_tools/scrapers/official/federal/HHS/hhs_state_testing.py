import pandas as pd
from can_tools.scrapers.official.base import ETagCacheMixin
from can_tools.scrapers.official.federal.HHS.common import HHSDataset

from can_tools.scrapers.variables import (
    CUMULATIVE_NEGATIVE_TEST_SPECIMENS,
    CUMULATIVE_POSITIVE_TEST_SPECIMENS,
)


class HHSTestingState(HHSDataset, ETagCacheMixin):
    has_location = True
    location_type = "state"
    fetch_url = (
        "https://healthdata.gov/api/views/j8mb-icvb/rows.csv?accessType=DOWNLOAD"
    )
    source = "https://healthdata.gov/dataset/COVID-19-Diagnostic-Laboratory-Testing-PCR-Testing/j8mb-icvb"

    variables = {
        "Negative": CUMULATIVE_NEGATIVE_TEST_SPECIMENS,
        "Positive": CUMULATIVE_POSITIVE_TEST_SPECIMENS,
    }

    def __init__(self, execution_dt: pd.Timestamp = pd.Timestamp.utcnow()):
        ETagCacheMixin.initialize_cache(
            self,
            cache_url=self.fetch_url,
            cache_file="hhs_state_testing.txt",
        )
        super().__init__(execution_dt=execution_dt)

    def fetch(self) -> pd.DataFrame:
        return pd.read_csv(self.fetch_url)

    def normalize(self, data: pd.DataFrame) -> pd.DataFrame:
        return (
            # Do not track inconclusive tests or data from the Marshall Islands
            data.loc[
                (data["overall_outcome"] != "Inconclusive") & (data["state_fips"] != 68)
            ]
            .rename(
                columns={
                    "date": "dt",
                    "state_fips": "location",
                    "total_results_reported": "value",
                }
            )
            .pipe(self.extract_CMU, cmu=self.variables, var_name="overall_outcome")
            .assign(vintage=self._retrieve_vintage())
        )
