import numpy as np
import pandas as pd

from can_tools.scrapers import CMU
from can_tools.scrapers.official.DC.dc_base import DCBase


class DCDeaths(DCBase):
    def _wrangle(self, data):
        """
        inherited helper function to transform excel data into standard dataframe format

        accepts: Pd.Dataframe
        returns: Pd.Dataframe
        """
        df = data.transpose()
        df = (
            df.rename(columns=df.iloc[0])
            .drop(df.index[0])
            .rename_axis("dt")
            .reset_index()  # make rownames into column
        )
        df["location_name"] = "District of Columbia"

        return self._make_dt_date_and_drop(df)

    def normalize(self, data):
        # retrieve data and convert dataframe structure
        data = pd.ExcelFile(data.content)
        df_age = self._wrangle(data.parse("Lives Lost by Age"))
        df_sex = self._wrangle(data.parse("Lives Lost by Sex"))
        df_race = self._wrangle(data.parse("Lives Lost by Race"))

        # maps for each df
        crename_age = {
            "<19": CMU(
                category="deaths",
                measurement="cumulative",
                unit="people",
                age="0-19",
            ),
            "20-29": CMU(
                category="deaths",
                measurement="cumulative",
                unit="people",
                age="20-29",
            ),
            "30-39": CMU(
                category="deaths",
                measurement="cumulative",
                unit="people",
                age="30-39",
            ),
            "40-49": CMU(
                category="deaths",
                measurement="cumulative",
                unit="people",
                age="40-49",
            ),
            "50-59": CMU(
                category="deaths",
                measurement="cumulative",
                unit="people",
                age="50-59",
            ),
            "60-69": CMU(
                category="deaths",
                measurement="cumulative",
                unit="people",
                age="60-69",
            ),
            "70-79": CMU(
                category="deaths",
                measurement="cumulative",
                unit="people",
                age="70-79",
            ),
            "80+": CMU(
                category="deaths",
                measurement="cumulative",
                unit="people",
                age="80_plus",
            ),
        }
        crename_sex = {
            "Female": CMU(
                category="deaths",
                measurement="cumulative",
                unit="people",
                sex="female",
            ),
            "Male": CMU(
                category="deaths",
                measurement="cumulative",
                unit="people",
                sex="male",
            ),
        }
        crename_race = {
            "Asian": CMU(
                category="deaths",
                measurement="cumulative",
                unit="people",
                race="asian",
            ),
            "Black/African American": CMU(
                category="deaths",
                measurement="cumulative",
                unit="people",
                race="black",
            ),
            "Hispanic/Latinx": CMU(
                category="deaths",
                measurement="cumulative",
                unit="people",
                race="all",
                ethnicity="hispanic",
            ),
            "Non-Hispanic White": CMU(
                category="deaths",
                measurement="cumulative",
                unit="people",
                race="white",
                ethnicity="non-hispanic",
            ),
            "Unknown": CMU(
                category="deaths",
                measurement="cumulative",
                unit="people",
                race="unknown",
            ),
            "All": CMU(
                category="deaths",
                measurement="cumulative",
                unit="people",
            ),
        }

        # rename and add columns according to map
        df_age = self._reshape(df_age, crename_age)
        df_sex = self._reshape(df_sex, crename_sex)
        df_race = self._reshape(df_race, crename_race)

        # combine all into one df
        df = pd.DataFrame()
        df = (
            df.append(df_age, ignore_index=True)
            .append(df_sex, ignore_index=True)
            .append(df_race, ignore_index=True)
        )

        # we have two dt that are pretty much identical
        # they are:
        #   numpy.datetime64('2020-06-07T00:00:00.100000000')
        #   numpy.datetime64('2020-06-07T00:00:00.000000000')
        # We drop one of them
        bad_dates = [
            np.datetime64("2021-04-11T00:00:00.100000000"),
            np.datetime64("2020-06-07T00:00:00.100000000"),
        ]
        bad = df["dt"].isin(bad_dates)

        return df.loc[~bad, :]
