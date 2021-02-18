import numpy as np
import pandas as pd
import us

from can_tools.scrapers.base import CMU
from can_tools.scrapers.official.base import StateDashboard


class NorthCarolinaVaccineCounty(StateDashboard):
    has_location = False
    source = "https://covid19.ncdhhs.gov/dashboard/vaccinations"
    source_name = (
        "North Carolina Department of Health and Human Services Covid-19 Response"
    )
    state_fips = int(us.states.lookup("North Carolina").fips)
    url = "https://files.nc.gov/covid/documents/dashboard/Vaccinations_Dashboard_Data.xlsx"
    location_type = "county"

    def fetch(self):
        return pd.read_excel(self.url, sheet_name="Vaccinations Data")

    def normalize(self, data: pd.DataFrame) -> pd.DataFrame:
        # date is written out in first column name
        dt = pd.to_datetime(
            list(data)[0].split("dashboard")[0].strip(),
            format="Data for the %b. %d, %Y",
        )

        # find row with column labels
        colnames_ix = (data.iloc[:, 0] == "County").idxmax()
        data.columns = pd.Index(data=data.iloc[colnames_ix, :], name="")
        cmus = {
            "Dose 1 Administered": CMU(
                category="total_vaccine_initiated",
                measurement="cumulative",
                unit="people",
            ),
            "Dose 2 Administered": CMU(
                category="total_vaccine_completed",
                measurement="cumulative",
                unit="people",
            ),
        }

        # Reformat
        col_renamer = {
            "County": "location_name",
            "Vaccine Status": "variable",
            "Total Doses": "value",
        }
        df = (
            data.iloc[(colnames_ix + 1) :, :]
            .rename(columns=col_renamer)
            .loc[:, col_renamer.values()]
        )

        # Fix names of counties...
        df.loc[:, "location_name"] = (
            df.loc[:, "location_name"].str.strip().replace({"Mcdowell": "McDowell"})
        )

        # Drop missing
        df = (
            df.dropna()
            .replace({"value": {" ": np.nan}})
            .query("location_name != 'Missing'")
        )

        df = df.assign(
            value=lambda x: pd.to_numeric(x.loc[:, "value"]),
            vintage=self._retrieve_vintage(),
            dt=dt,
        )

        out = self.extract_CMU(df, cmus).drop(["variable"], axis="columns")

        return out
