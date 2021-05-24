import io
from abc import ABC

import lxml.html
import pandas as pd
import requests
import us

from can_tools.scrapers import CMU
from can_tools.scrapers.official.base import StateDashboard

# the crename keys became long so I store them in another file
from can_tools.scrapers.official.TX.tx_vaccine_crenames import crename


class TexasVaccineParent(StateDashboard, ABC):
    state_fips = int(us.states.lookup("Texas").fips)
    source = "https://www.dshs.state.tx.us/coronavirus/immunize/vaccine.aspx"
    source_name = "Texas Department of State Health Services"

    def fetch(self) -> requests.models.Response:
        fetch_url = "https://www.dshs.state.tx.us/immunize/covid19/COVID-19-Vaccine-Data-by-County.xls"
        res = requests.get(fetch_url)
        if not res.ok:
            raise ValueError("Could not fetch download file")
        return res

    def excel_to_dataframe(self, data, sheet) -> pd.DataFrame:
        # Read data from excel file and parse specified sheet
        return pd.read_excel(
            data.content, sheet_name=sheet, engine="openpyxl", na_values="--"
        ).assign(dt=self._retrieve_dtm1d("US/Eastern"))


class TexasCountyVaccine(TexasVaccineParent):
    has_location = False
    location_type = "county"

    def _rename_and_reshape(self, df):
        # Rename columns
        df = df.rename(columns={"County Name": "location_name"})

        # Exclude other locations
        df = df.query(
            "location_name != '*Other' & "
            "location_name != 'Federal Long-Term Care Vaccination Program'"
        )

        # Reshape and set values to numeric types
        out = df.melt(
            id_vars=["dt", "location_name"], value_vars=crename.keys()
        ).dropna()
        out.loc[:, "value"] = pd.to_numeric(out["value"])
        out = self.extract_CMU(out, crename)

        out["vintage"] = self._retrieve_vintage()

        return out

    def normalize(self, data) -> pd.DataFrame:
        # Read excel file and set date
        df = self.excel_to_dataframe(data, "By County")
        df = self._rename_and_reshape(df)
        non_counties = [
            "Texas",
            "Federal Pharmacy Retail Vaccination Program",
            "Other",
            "Grand Total",
            "* Other",
        ]
        # Drop state data which we retrieve with another scraper
        # Drop data where location_name is "Federal Pharmacy Retail Vaccination Program"
        df = df.query("location_name not in @non_counties")

        cols_to_keep = [
            "vintage",
            "dt",
            "location_name",
            "category",
            "measurement",
            "unit",
            "age",
            "race",
            "ethnicity",
            "sex",
            "value",
        ]
        return df.loc[:, cols_to_keep]


class TexasStateVaccine(TexasCountyVaccine):
    has_location = True
    location_type = "state"
    # just statewide data

    def normalize(self, data) -> pd.DataFrame:
        # Read excel file and set date
        df = self.excel_to_dataframe(data, "By County")
        df = self._rename_and_reshape(df)

        # Drop state data which we retrieve with another scraper
        df = df.query("location_name == 'Texas'")
        df["location"] = self.state_fips

        cols_to_keep = [
            "vintage",
            "dt",
            "location",
            "category",
            "measurement",
            "unit",
            "age",
            "race",
            "ethnicity",
            "sex",
            "value",
        ]
        return df.loc[:, cols_to_keep]


class TXVaccineCountyAge(TexasVaccineParent):
    location_type = "county"
    has_location = False
    cmus = {
        "Doses Administered": CMU(
            category="total_vaccine_doses_administered",
            measurement="cumulative",
            unit="doses",
        ),
        "People Vaccinated with at least One Dose": CMU(
            category="total_vaccine_initiated",
            measurement="cumulative",
            unit="people",
        ),
        "People Fully Vaccinated": CMU(
            category="total_vaccine_completed",
            measurement="cumulative",
            unit="people",
        ),
    }
    cmu_id_vars = ["age"]
    sheet_name = "By County, Age"
    replacers = {
        "age": {
            "12-15 years": "12-15",
            "16-49 years": "16-49",
            "50-64 years": "50-64",
            "65-79 years": "65-79",
            "80+ years": "80_plus",
            "Unknown": "unknown",
            "Total": "all",
        },
        "race": {
            "American Indian/Alaskan Native": "ai_an",
            "Asian": "asian",
            "Black": "black",
            "Multiple Races": "multiple",
            "Native Hawaiian/Other Pacific Islander": "pacific_islander",
            "Other": "other",
            "Unknown Race": "unknown",
            "Unknown": "unknown",
            "White": "white",
        },
        "ethnicity": {
            "Hispanic": "hispanic",
            "Not Hispanic": "non-hispanic",
            "Unknown": "unknown",
        },
        "sex": {
            "Female": "female",
            "Male": "male",
            "Unknown": "unknown",
        },
    }

    @property
    def cmu_columns(self):
        return list(
            set(["category", "measurement", "unit", "age", "race", "sex", "ethnicity"])
            - set(self.cmu_id_vars)
        )

    def normalize(self, data) -> pd.DataFrame:
        # Read in data, set location, and drop totals
        non_counties = ["Other", "Grand Total"]
        df = (
            self.excel_to_dataframe(data, self.sheet_name)
            .rename(columns=str.strip)
            .rename(
                columns={
                    "Age Group": "age",
                    "Race/Ethnicity": "race",
                    "County Name": "location_name",
                }
            )
            .rename(columns=str.strip)
            .melt(
                id_vars=["dt", "location_name"] + self.cmu_id_vars,
                value_vars=list(self.cmus.keys()),
            )
            .replace(self.replacers)
            .pipe(self.extract_CMU, cmu=self.cmus, columns=self.cmu_columns)
            .pipe(lambda x: x.loc[~x["location_name"].isin(["*Other", "Total"]), :])
            .assign(vintage=self._retrieve_vintage())
            .query("location_name not in @non_counties")
            .dropna(subset=["value"])
        )

        return df


class TXVaccineCountyRace(TXVaccineCountyAge):
    cmu_id_vars = ["race"]
    sheet_name = "By County, Race"

    def normalize(self, data) -> pd.DataFrame:
        df = super().normalize(data)
        hisp_rows = df["race"] == "Hispanic"
        df.loc[hisp_rows, "ethnicity"] = "hispanic"
        df.loc[hisp_rows, "race"] = "unknown"

        return df
