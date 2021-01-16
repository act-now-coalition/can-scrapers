import pandas as pd
import us
import lxml.html
import requests
from can_tools.scrapers import CMU
from can_tools.scrapers.official.base import StateDashboard

# the crename keys became long so I store them in another file
from .tx_vaccine_crenames import crename_demographics, crename


class TexasVaccine(StateDashboard):
    has_location = False
    state_fips = us.states.lookup("Texas").fips
    location_type = "county"
    source = "https://www.dshs.state.tx.us/coronavirus/immunize/vaccine.aspx"

    def fetch(self) -> requests.models.Response:
        fetch_url = "https://www.dshs.state.tx.us/immunize/covid19/COVID-19-Vaccine-Data-by-County.xls"
        res = requests.get(fetch_url)
        if not res.ok:
            raise ValueError("Could not fetch download file")
        return res

    def normalize(self, data) -> pd.DataFrame:
        data = pd.ExcelFile(data.content)
        df = data.parse("By County").rename(columns={"County Name": "location_name"})
        df["dt"] = self._retrieve_dtm1d("US/Eastern")
        # currenty ignores statewide data
        df = df[(df["location_name"] != "*Other") & (df["location_name"] != "Texas")]

        out = df.melt(
            id_vars=["dt", "location_name"], value_vars=crename.keys()
        ).dropna()
        out.loc[:, "value"] = pd.to_numeric(out["value"])
        out = self.extract_CMU(out, crename)
        out["vintage"] = self._retrieve_vintage()

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
        return out.loc[:, cols_to_keep]


class TexasVaccineDemographics(TexasVaccine):
    location_type = "state"
    has_location = True

    def normalize(self, data) -> pd.DataFrame:
        data = pd.ExcelFile(data.content)
        df = data.parse("Vaccinations by Gender, Age")
        df["dt"] = self._retrieve_dtm1d("US/Eastern")
        df["location"] = self.state_fips

        # if the number of rows for each gender are changed this will break
        df_female = df.iloc[0:6, :]
        df_male = df.iloc[6:12, :]
        df_unknown = df.iloc[12:18, :]

        df_female = self._reshape(df_female, "female")
        df_male = self._reshape(df_male, "male")
        df_unknown = self._reshape(df_unknown, "unknown")

        df = pd.DataFrame()
        df = pd.concat([df_female, df_male, df_unknown], axis=0, ignore_index=True)
        return df

    def _reshape(self, data, sex) -> pd.DataFrame:
        # variables we want to track
        columns = [
            "Doses Administered",
            "People Vaccinated with at least One Dose",
            "People Fully Vaccinated",
        ]

        # use Age Group as an ID var to keep each age group distinct for each variable in columns
        out = data.melt(
            id_vars=["dt", "location", "Age Group"], value_vars=columns
        ).dropna()

        # then recombine so we can replace the variable with a CMU pair, while keeping the age buckets
        # Age Group is dropped before returning
        out["variable"] = out["variable"] + " " + out["Age Group"]
        out.loc[:, "value"] = pd.to_numeric(out["value"])
        out = self.extract_CMU(out, crename_demographics)
        out["sex"] = sex
        out["vintage"] = self._retrieve_vintage()

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
        return out.loc[:, cols_to_keep]
