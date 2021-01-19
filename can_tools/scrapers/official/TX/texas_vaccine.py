import io

from abc import ABC

import pandas as pd
import us
import lxml.html
import requests

from can_tools.scrapers import CMU
from can_tools.scrapers.official.base import StateDashboard

# the crename keys became long so I store them in another file
from .tx_vaccine_crenames import crename_demographics, crename


class TexasVaccineParent(StateDashboard, ABC):
    state_fips = us.states.lookup("Texas").fips
    source = "https://www.dshs.state.tx.us/coronavirus/immunize/vaccine.aspx"

    def fetch(self) -> requests.models.Response:
        fetch_url = "https://www.dshs.state.tx.us/immunize/covid19/COVID-19-Vaccine-Data-by-County.xls"
        res = requests.get(fetch_url)
        if not res.ok:
            raise ValueError("Could not fetch download file")
        return res

    def excel_to_dataframe(self, data, sheet) -> pd.DataFrame:
        # Read data from excel file and parse specified sheet
        data = pd.ExcelFile(io.BytesIO(data.content))
        df = data.parse(sheet, na_values="--")

        # Set date to yesterday
        df["dt"] = self._retrieve_dtm1d("US/Eastern")

        return df


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

        # Drop state data which we retrieve with another scraper
        df = df.query("location_name != 'Texas'")

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


# class TexasVaccineDemographics(TexasVaccineParent):
#     location_type = "state"
#     has_location = True
#
#     def compute_new_groups(self, df, cols_to_combine):
#         return df.groupby(cols_to_combine)["value"].sum()
#
#     def normalize(self, data) -> pd.DataFrame:
#         # Read in data, set location, and drop totals
#         df = self.excel_to_dataframe(data, "By Age, Gender, Race")
#         df["location"] = self.state_fips
#         df.query("Gender != 'Texas'")
#
#         # Data is now stored long-form -- We can simply rename columns and their
#         # values to get what we want
#         df = df.rename(columns={
#             "Gender": "sex",
#             "Age Group": "age",
#             "Race": "race",
#             "Ethnicity": "ethnicity",
#         })
#         out = df.melt(id_vars=["dt", "location", "age", "race", "ethnicity", "sex"])
#         out = out.replace({
#             "age": {
#                 "16-49 years": "16-49",
#                 "50-64 years": "50-64",
#                 "65-79 years": "65-79",
#                 "80+ years": "80+",
#                 "Unknown": "unknown",
#                 "Total": "all"
#             },
#             "race": {
#                 "American Indian/Alaskan Native": "ai_an",
#                 "Asian": "asian",
#                 "Black": "black",
#                 "Multiple Races": "multiple",
#                 "Native Hawaiian/Other Pacific Islander": "pacific_islander",
#                 "Other": "other",
#                 "Unknown Race": "unknown",
#                 "White": "white"
#             },
#             "ethnicity": {
#                 "Hispanic": "hispanic",
#                 "Not Hispanic": "non-hispanic",
#                 "Unknown": "unknown"
#             },
#             "sex": {
#                 "Female": "female",
#                 "Male": "male",
#                 "Unknown": "unknown",
#             }
#         })
#
#         # Set category, measurement, unit -- demographics already set
#         out = self.extract_CMU(
#             out, crename_demographics,
#             columns=["category", "measurement", "unit"]
#         )
#
#         # Create combinations of variables
#         base_cols = ["dt", "location", "category", "measurement", "unit"]
#
#         all_age = self.compute_new_groups(
#             out, base_cols + ["sex", "race", "ethnicity"]
#         )
#         all_age["age"] = "all"
#
#         all_eth = self.compute_new_groups(
#             out, base_cols + ["age", "sex", "race"]
#         )
#         all_eth["ethnicity"] = "all"
#
#         all_race = self.compute_new_groups(
#             out, base_cols + ["age", "sex", "ethnicity"]
#         )
#         all_race["race"] = "all"
#
#         all_sex = self.compute_new_groups(
#             out, base_cols + ["age", "race", "ethnicity"]
#         )
#         all_sex["sex"] = "all"
#
#         out = pd.concat([out, all_age, all_eth, all_race, all_sex])
#         out["vintage"] = self._retrieve_vintage()
#         cols_to_keep = [
#             "vintage",
#             "dt",
#             "location",
#             "category",
#             "measurement",
#             "unit",
#             "age",
#             "race",
#             "ethnicity",
#             "sex",
#             "value",
#         ]
#         return out.loc[:, cols_to_keep]
