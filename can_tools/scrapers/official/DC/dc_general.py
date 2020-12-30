import pandas as pd
from can_tools.scrapers import CMU
from can_tools.scrapers.official.DC.dc_base import DCBase


class DCGeneral(DCBase):
    def _wrangle(self, data):
        """
        inherited helper function to transform raw data into standard dataframe format

        accepts: Pandas df
        returns: Pandas df
        """
        df = (
            data.head(n=16)
            .drop(["Unnamed: 0"], axis=1)
            .transpose()  # flip the rows to cols
        )
        df = (
            df.drop([0], axis=1)
            .rename(columns=df.iloc[0])
            .drop(df.index[0])
            .rename_axis("dt")
            .reset_index()  # make rownames into column
        )
        df["location_name"] = "District of Columbia"
        return df

    def normalize(self, data):
        # retrieve and convert excel object to df, re-structure df
        df = self._wrangle(data.parse("Overal Stats"))

        crename = {
            "Total Overall Number of Tests": CMU(
                category="unspecified_tests_total",
                measurement="cumulative",
                unit="test_encounters",
            ),
            "Total Number of DC Residents Tested": CMU(
                category="unspecified_tests_total",
                measurement="cumulative",
                unit="unique_people",
            ),
            "Total ICU Beds in Hospitals": CMU(
                category="icu_beds_capacity",
                measurement="current",
                unit="beds",
            ),
            "ICU Beds Available": CMU(
                category="icu_beds_available",
                measurement="current",
                unit="beds",
            ),
            "Total Reported Ventilators in Hospitals": CMU(
                category="ventilators_capacity",
                measurement="current",
                unit="people",
            ),
            "In-Use Ventilators in Hospitals": CMU(
                category="ventilators_in_use",
                measurement="current",
                unit="people",
            ),
            "Available Ventilators in Hospitals": CMU(
                category="ventilators_available",
                measurement="current",
                unit="people",
            ),
            "Total COVID-19 Patients in ICU": CMU(  ##check
                category="icu_beds_in_use_covid",
                measurement="current",
                unit="beds",
            ),
        }

        # return df in correct format for put() with new/renamed cols
        return self._reshape(df, crename)
