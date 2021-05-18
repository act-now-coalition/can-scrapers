import pandas as pd
import requests
import us

from can_tools.scrapers.base import CMU
from can_tools.scrapers.official.base import StateDashboard
from can_tools.scrapers.util import requests_retry_session


class TennesseeBase(StateDashboard):
    state_fips = int(us.states.lookup("Tennessee").fips)
    source_name = "Tennessee Department of Health"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.session = requests_retry_session()

    def translate_age(self, df: pd.DataFrame):
        df = df.replace(
            {
                "age": {
                    "0-10 years": "0-10",
                    "11-20 years": "11-20",
                    "21-30 years": "21-30",
                    "31-40 years": "31-40",
                    "41-50 years": "41-50",
                    "51-60 years": "51-60",
                    "61-70 years": "61-70",
                    "71-80 years": "71-80",
                    "81+ years": "81_plus",
                }
            }
        )

        return df


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
        return request

    def normalize(self, data) -> pd.DataFrame:
        # Read data into data frame
        df = pd.read_excel(data.content, parse_dates=["DATE"])

        # Rename columns
        df = df.rename(columns={"DATE": "dt", "AGE_RANGE": "age"})

        # Translate age column
        df = self.translate_age(df)

        # Create dictionary for columns to map
        crename = {
            "AR_TOTALCASES": CMU(
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
            columns=["category", "measurement", "unit", "race", "ethnicity", "sex"],
        )

        out = df.query("age != 'Pending'").drop(["variable"], axis=1)

        # Convert value columns
        out["value"] = out["value"].astype(int)

        # Add rows that don't change
        out["location"] = self.state_fips
        out["vintage"] = self._retrieve_vintage()

        return out


class TennesseeAgeByCounty(TennesseeBase):
    """
    Fetch county level Covid-19 data by age from official state of Tennessee spreadsheet
    """

    source = (
        "https://www.tn.gov/content/tn/health/cedep/ncov/data.html"
        "https://www.tn.gov/health/cedep/ncov/data/downloadable-datasets.html"
    )
    has_location = False
    location_type = "county"
    url = (
        "https://www.tn.gov/content/dam/tn/health/documents/cedep/"
        "novel-coronavirus/datasets/Public-Dataset-Daily-County-Age-Group.XLSX"
    )

    def fetch(self) -> requests.Response:
        # Set url of downloadable dataset
        request = self.session.get(self.url)

        return request

    def normalize(self, data) -> pd.DataFrame:
        # Read data into data frame
        df = pd.read_excel(data.content, parse_dates=["date"])

        # Rename columns
        df = df.rename(
            columns={"date": "dt", "COUNTY": "location_name", "AGE_GROUP": "age"}
        )

        # Drop the information that we won't be keeping track of
        age_not_keep = ["Pending"]
        loc_not_keep = ["Out of State", "Pending"]
        df = df.query(
            "(age not in @age_not_keep) & (location_name not in @loc_not_keep)"
        )

        # Fix incorrectly spelled county names
        loc_replacer = {"Dekalb": "DeKalb"}
        df = df.replace({"location_name": loc_replacer})

        # Translate age column
        df = self.translate_age(df)

        # Create dictionary for columns to map
        crename = {
            "TOTAL_CASES": CMU(
                category="cases", measurement="cumulative", unit="people"
            ),
        }

        # Move things into long format
        df = df.melt(
            id_vars=["dt", "location_name", "age"], value_vars=crename.keys()
        ).dropna()

        # Determine the category of each observation
        df = self.extract_CMU(
            df, crename, ["category", "measurement", "unit", "race", "ethnicity", "sex"]
        )

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

        # Convert value columns
        out["value"] = out["value"].astype(int)

        # Add rows that don't change
        out["vintage"] = self._retrieve_vintage()

        return out


class TennesseeRaceEthnicitySex(TennesseeBase):
    """
    Fetch state level Covid-19 data by race, ethnicity, and gender from official state of Tennessee spreadsheet
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
            "novel-coronavirus/datasets/Public-Dataset-RaceEthSex.XLSX"
        )
        request = self.session.get(url)

        return request

    def normalize(self, data) -> pd.DataFrame:
        # Read data into data frame
        df = pd.read_excel(data.content, parse_dates=["Date"])

        # Rename columns
        df = df.rename(
            columns={
                "Date": "dt",
                "CAT_DETAIL": "category_detail",
                "Category": "category_name",
            }
        )

        # Drop the information that we won't be keeping track of
        cat_detail_not_keep = ["Pending"]
        df = df.query("category_detail not in @cat_detail_not_keep")

        # Translate race, ethnicity, and gender (sex) to standard names
        cat_detail_replace = {
            "American Indian or Alaska Native": "ai_an",
            "Asian": "asian",
            "Black or African American": "black",
            "White": "white",
            "Native Hawaiian or Other Pacific Islander": "pacific_islander",
            "Other/ Multiracial": "multiple_other",
            "Other/Multiracial": "multiple_other",
            "Hispanic": "hispanic",
            "Not Hispanic or Latino": "non-hispanic",
            "Female": "female",
            "Male": "male",
        }
        df = df.replace({"category_detail": cat_detail_replace})

        # Split data packed into category_name and category_detail into race, ethnicity, and gender (sex) columns
        df.loc[df["category_name"] == "RACE", "race"] = df["category_detail"]
        df.loc[df["category_name"] != "RACE", "race"] = "all"
        df.loc[df["category_name"] == "ETHNICITY", "ethnicity"] = df["category_detail"]
        df.loc[df["category_name"] != "ETHNICITY", "ethnicity"] = "all"
        df.loc[df["category_name"] == "SEX", "sex"] = df["category_detail"]
        df.loc[df["category_name"] != "SEX", "sex"] = "all"

        # Create dictionary for columns to map
        crename = {
            "CAT_TOTALCASES": CMU(
                category="cases", measurement="cumulative", unit="people"
            ),
            "CAT_TOTALDEATHS": CMU(
                category="deaths", measurement="cumulative", unit="people"
            ),
        }

        # Move things into long format
        df = df.melt(
            id_vars=[
                "dt",
                "category_detail",
                "category_name",
                "race",
                "ethnicity",
                "sex",
            ],
            value_vars=crename.keys(),
        ).dropna()

        # Determine the category of each observation
        df = self.extract_CMU(df, crename, ["category", "measurement", "unit", "age"])

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
