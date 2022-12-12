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
        df = pd.read_excel(data.content, parse_dates=["DATE"])

        # Rename columns
        df = df.rename(
            columns={"DATE": "dt", "COUNTY": "location_name", "AGE_GROUP": "age"}
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
