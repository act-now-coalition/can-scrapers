import io
import zipfile
from typing import Optional

import pandas as pd
import requests
import us

from can_tools.scrapers.base import CMU, DatasetBaseNeedsDate
from can_tools.scrapers.official.base import StateDashboard


class Massachusetts(DatasetBaseNeedsDate, StateDashboard):
    start_date = "2019-04-29"
    source = (
        "https://www.mass.gov/info-details/"
        "covid-19-response-reporting#covid-19-daily-dashboard-"
    )
    state_fips = int(us.states.lookup("Massachusetts").fips)
    has_location = False

    def transform_date(self, date: pd.Timestamp) -> pd.Timestamp:
        return date - pd.Timedelta(hours=11)

    def _get_cases_deaths(self, zf: zipfile.ZipFile) -> pd.DataFrame:
        with zf.open("County.csv") as csv_f:
            df = pd.read_csv(csv_f, parse_dates=["Date"])

        names = {"Date": "dt", "County": "county"}
        cats = {
            "Total Confirmed Cases": CMU(
                category="cases", measurement="cumulative", unit="people"
            ),
            "Total Probable and Confirmed Deaths": CMU(
                category="deaths", measurement="cumulative", unit="people"
            ),
        }
        out = df.rename(columns=names).loc[:, list(names.values()) + list(cats.keys())]
        melted = out.melt(id_vars=["dt", "county"], value_vars=list(cats.keys()))

        return (
            melted.drop_duplicates(subset=["dt", "county", "variable"], keep="first")
            .pipe(self.extract_CMU, cats)
            .drop(["variable"], axis=1)
            .assign(value=lambda x: x["value"].fillna(-1).astype(int))
        )

    def _get_hospital_data(
        self, zf: zipfile.ZipFile, date: pd.Timestamp
    ) -> Optional[pd.DataFrame]:
        fn = "HospCensusBedAvailable.xlsx"
        if fn not in [x.filename for x in zf.filelist]:
            print("The file HospCensusBedAvailable.xlsx could not be found, skipping")
            return None
        with zf.open(fn, "r") as f:
            # NOTE: needed b/c python 3.6 doesn't have `.seek` method on
            # zipfile objects and pandas expects it
            content = io.BytesIO(f.read())
            df = pd.read_excel(content, sheet_name="Hospital COVID Census")

        hosp_col = [x for x in list(df) if "Including ICU" in x]
        if len(hosp_col) != 1:
            raise ValueError(
                f"Could not find total hospital column from list: {list(df)}"
            )

        icu_col = [x for x in list(df) if "ICU Census" in x]
        if len(icu_col) != 1:
            raise ValueError(f"Could not find ICU column from list: {list(df)}")

        cats = {
            hosp_col[-1]: CMU(
                category="hospital_beds_in_use_covid",
                measurement="current",
                unit="beds",
            ),
            icu_col[-1]: CMU(
                category="icu_beds_in_use_covid", measurement="current", unit="beds"
            ),
        }

        return (
            df.groupby("Hospital County")[list(cats.keys())]
            .sum()
            .reset_index()
            .rename(columns={"Hospital County": "county"})
            .assign(dt=date)
            .melt(id_vars=["dt", "county"])
            .pipe(self.extract_CMU, cats)
            .drop(["variable"], axis=1)
        )

    def get(self, date: str) -> pd.DataFrame:
        dt = pd.to_datetime(date)
        ds = dt.strftime("%B-%-d-%Y").lower()
        url = f"https://www.mass.gov/doc/covid-19-raw-data-{ds}/download"
        res = requests.get(url)
        if not res.ok:
            msg = f"Failed request to {url} with code{res.status_code} and message {res.content}"
            raise ValueError(msg)

        buf = io.BytesIO(res.content)
        buf.seek(0)
        dfs = []
        with zipfile.ZipFile(buf) as f:
            dfs.append(self._get_hospital_data(f, dt))
            dfs.append(self._get_cases_deaths(f))

        df = pd.concat([x for x in dfs if x is not None], ignore_index=True)
        df["value"] = df["value"].astype(int)
        df["vintage"] = self._retrieve_vintage()
        return df
