import pandas as pd
import requests
import us

from can_tools.scrapers.base import CMU
from can_tools.scrapers.official.base import StateDashboard
from can_tools.scrapers.util import requests_retry_session


class TennesseeBase(StateDashboard):
    state_fips = int(us.states.lookup("Tennessee").fips)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.session = requests_retry_session()

    def translate_age(self, df: pd.DataFrame):
        df.loc[df["age"] == "0-10 years", "age"] = "0-10"
        df.loc[df["age"] == "11-20 years", "age"] = "11-20"
        df.loc[df["age"] == "21-30 years", "age"] = "21-30"
        df.loc[df["age"] == "31-40 years", "age"] = "31-40"
        df.loc[df["age"] == "41-50 years", "age"] = "41-50"
        df.loc[df["age"] == "51-60 years", "age"] = "51-60"
        df.loc[df["age"] == "61-70 years", "age"] = "61-70"
        df.loc[df["age"] == "71-80 years", "age"] = "71-80"
        df.loc[df["age"] == "81+ years", "age"] = "81_plus"


class TennesseeState(TennesseeBase):
    """
    Fetch state level Covid-19 data from official state of Tennessee spreadsheet
    """

    source = (
        "https://www.tn.gov/content/tn/health/cedep/ncov/data.html"
        "https://www.tn.gov/health/cedep/ncov/data/downloadable-datasets.html"
    )
    has_location = True
    location_type = "state"

    def fetch(self) -> requests.Response:
        # Set url of downloadable dataset
        url = (
            "https://www.tn.gov/content/dam/tn/health/documents/cedep/"
            "novel-coronavirus/datasets/Public-Dataset-Daily-Case-Info.XLSX"
        )
        request = self.session.get(url)
        return request.content

    def normalize(self, data) -> pd.DataFrame:
        # Read data into data frame
        df = pd.read_excel(data, parse_dates=["DATE"])

        # Rename columns
        df = df.rename(columns={"DATE": "dt"})

        # Create dictionary for columns to map
        crename = {
            "TOTAL_CONFIRMED": CMU(
                category="cases", measurement="cumulative", unit="people"
            ),
            "NEW_CONFIRMED": CMU(category="cases", measurement="new", unit="people"),
            "TOTAL_DEATHS": CMU(
                category="deaths", measurement="cumulative", unit="people"
            ),
            "NEW_DEATHS": CMU(category="deaths", measurement="new", unit="people"),
            "POS_TESTS": CMU(
                category="pcr_tests_positive",
                measurement="cumulative",
                unit="specimens",
            ),
            "NEG_TESTS": CMU(
                category="pcr_tests_negative",
                measurement="cumulative",
                unit="specimens",
            ),
            "TOTAL_TESTS": CMU(
                category="pcr_tests_total",
                measurement="cumulative",
                unit="specimens",
            ),
        }

        # Move things into long format
        df = df.melt(id_vars=["dt"], value_vars=crename.keys()).dropna()

        # Determine the category of each observation
        df = self.extract_CMU(df, crename)

        # Determine what columns to keep
        cols_to_keep = [
            "dt",
            "category",
            "measurement",
            "unit",
            "age",
            "race",
            "ethnicity",
            "sex",
            "value",
        ]

        # Drop extraneous columns
        out = df.loc[:, cols_to_keep]

        # Convert value columns
        out["value"] = out["value"].astype(int)

        # Add rows that don't change
        out["location"] = self.state_fips
        out["vintage"] = self._retrieve_vintage()

        return out

    def validate(self, df, df_hist) -> bool:
        "The best developers write test cases first..."
        # TODO: Create a calculated column and sanity check this against unspecified_tests_total
        # df["unspecified_tests_total_calculated"] = df.eval("unspecified_tests_positive + unspecified_tests_negative")
        return True


class TennesseeCounty(TennesseeBase):
    """
    Fetch county level Covid-19 data from official state of Tennessee spreadsheet
    """

    source = (
        "https://www.tn.gov/content/tn/health/cedep/ncov/data.html"
        "https://www.tn.gov/health/cedep/ncov/data/downloadable-datasets.html"
    )
    has_location = False
    location_type = "county"

    def fetch(self) -> requests.Response:
        # Set url of downloadable dataset
        url = (
            "https://www.tn.gov/content/dam/tn/health/documents/cedep/"
            "novel-coronavirus/datasets/Public-Dataset-County-New.XLSX"
        )
        request = self.session.get(url)
        return request.content

    def normalize(self, data) -> pd.DataFrame:
        # Read data into data frame
        df = pd.read_excel(data, parse_dates=["DATE"])

        # Rename columns
        df = df.rename(columns={"DATE": "dt", "COUNTY": "location_name"})

        # Create dictionary for columns to map
        crename = {
            "TOTAL_CONFIRMED": CMU(
                category="cases", measurement="cumulative", unit="people"
            ),
            "NEW_CONFIRMED": CMU(category="cases", measurement="new", unit="people"),
            "POS_TESTS": CMU(
                category="pcr_tests_positive",
                measurement="cumulative",
                unit="specimens",
            ),
            "NEG_TESTS": CMU(
                category="pcr_tests_negative",
                measurement="cumulative",
                unit="specimens",
            ),
            "TOTAL_TESTS": CMU(
                category="pcr_tests_total",
                measurement="cumulative",
                unit="specimens",
            ),
            "NEW_DEATHS": CMU(category="deaths", measurement="new", unit="people"),
            "TOTAL_DEATHS": CMU(
                category="deaths", measurement="cumulative", unit="people"
            ),
            "NEW_HOSPITALIZED": CMU(
                category="hospital_beds_in_use_covid", measurement="new", unit="beds"
            ),
            "TOTAL_HOSPITALIZED": CMU(
                category="hospital_beds_in_use_covid",
                measurement="cumulative",
                unit="beds",
            ),
        }

        # Move things into long format
        df = df.melt(
            id_vars=["dt", "location_name"], value_vars=crename.keys()
        ).dropna()

        # Determine the category of each observation
        df = self.extract_CMU(df, crename)

        # Determine what columns to keep
        cols_to_keep = [
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

        # Drop extraneous columns
        out = df.loc[:, cols_to_keep]

        # Drop the information that we won't be keeping track of
        loc_not_keep = ["Out of State", "Pending"]
        out = out.loc[~out["location_name"].isin(loc_not_keep), :]

        # Fix incorrectly spelled county names
        out.loc[out["location_name"] == "Dekalb", "location_name"] = "DeKalb"

        # Convert value columns
        out["value"] = out["value"].astype(int)

        # Add rows that don't change
        out["vintage"] = self._retrieve_vintage()

        return out

    def validate(self, df, df_hist) -> bool:
        "Striving to better, oft we mar what's well. -Shakespeare"
        return True


class TennesseeAge(TennesseeBase):
    """
    Fetch state level Covid-19 data by age from official state of Tennessee spreadsheet
    """

    source = (
        "https://www.tn.gov/content/tn/health/cedep/ncov/data.html"
        "https://www.tn.gov/health/cedep/ncov/data/downloadable-datasets.html"
    )
    has_location = True
    location_type = "state"

    def fetch(self) -> requests.Response:
        # Set url of downloadable dataset
        url = (
            "https://www.tn.gov/content/dam/tn/health/documents/cedep/"
            "novel-coronavirus/datasets/Public-Dataset-Age.XLSX"
        )
        request = self.session.get(url)
        return request.content

    def normalize(self, data) -> pd.DataFrame:
        # Read data into data frame
        df = pd.read_excel(data, parse_dates=["DATE"])

        # Rename columns
        df = df.rename(columns={"DATE": "dt", "AGE_RANGE": "age"})

        # Translate age column
        self.translate_age(df)

        # Create dictionary for columns to map
        crename = {
            "AR_CASECOUNT": CMU(
                category="cases", measurement="cumulative", unit="people"
            ),
            "AR_TOTALDEATHS": CMU(
                category="deaths", measurement="cumulative", unit="people"
            ),
        }

        # Move things into long format
        df = df.melt(id_vars=["dt", "age"], value_vars=crename.keys()).dropna()

        # Determine the category of each observation
        df = self.extract_CMU(
            df,
            crename,
            columns=["category", "measurement", "unit", "race", "sex"],
        )

        out = df.query("age != 'Pending'").drop(["variable"], axis=1)

        # Convert value columns
        out["value"] = out["value"].astype(int)

        # Add rows that don't change
        out["location"] = self.state_fips
        out["vintage"] = self._retrieve_vintage()

        return out

    def validate(self, df, df_hist) -> bool:
        "The best is the enemy of the good. -Voltaire"
        return True
