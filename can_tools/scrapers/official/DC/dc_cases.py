import pandas as pd

from can_tools.scrapers import CMU
from can_tools.scrapers.official.DC.dc_base import DCBase


class DCCases(DCBase):
    def _wrangle(self, data):
        """
        inherited helper function to transform excel data into standard dataframe format

        accepts: Pd.Dataframe
        returns: Pd.Dataframe
        """
        df = data.head(n=10)
        df = df.transpose()
        df = df.set_index([0])
        df = df.rename(columns=df.iloc[0])
        df = df.drop(df.index[0])
        df = df.rename_axis("dt").reset_index()  # make rownames into column
        df["location_name"] = "District of Columbia"

        return self._make_dt_date_and_drop(df)

    def normalize(self, data):
        data = pd.ExcelFile(data.content)
        df = self._wrangle(data.parse("Total Cases by Race"))

        crename = {
            "All": CMU(
                category="cases",
                measurement="cumulative",
                unit="people",
            ),
            "Unknown": CMU(
                category="cases",
                measurement="cumulative",
                unit="people",
                race="unknown",
            ),
            "White": CMU(
                category="cases", measurement="cumulative", unit="people", race="white"
            ),
            "Black/African American": CMU(
                category="cases", measurement="cumulative", unit="people", race="black"
            ),
            "Asian": CMU(
                category="cases", measurement="cumulative", unit="people", race="asian"
            ),
            "American Indian/Alaska Native": CMU(  ##question?
                category="cases",
                measurement="cumulative",
                unit="people",
                race="ai_an",
            ),
            "Native Hawaiin Pacific Islander": CMU(  ##question?
                category="cases",
                measurement="cumulative",
                unit="people",
                race="pacific_islander",
            ),
            "Other/Multi-Racial": CMU(  ##question?
                category="cases",
                measurement="cumulative",
                unit="people",
                race="multiple_other",
            ),
        }
        return self._reshape(df, crename)
