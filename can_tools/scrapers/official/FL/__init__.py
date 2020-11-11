import pandas as pd
import pyppeteer
import us

from ...base import CMU, DatasetBaseNoDate
from ...puppet import TableauNeedsClick
from ..base import ArcGIS, CountyData


class FloridaHospitalUsage(TableauNeedsClick):

    url = "https://bi.ahca.myflorida.com/t/ABICC/views/Public/HospitalBedsHospital?:isGuestRedirectFromVizportal=y&:embed=y"

    async def _pre_click(self, page: pyppeteer.page.Page):
        await page.waitForSelector(".tabCanvas")

    def _clean_cols(self, df: pd.DataFrame, bads: list, str_cols: list) -> pd.DataFrame:
        for col in list(df):
            for bad in bads:
                df[col] = df[col].astype(str).str.replace(bad, "")
            if col not in str_cols:
                df[col] = pd.to_numeric(df[col])

        return df

    def _clean_df(self, df: pd.DataFrame) -> pd.DataFrame:
        bads = [r",", r"%", "nan"]
        str_cols = ["County", "FileNumber", "ProviderName"]
        df = self._clean_cols(df, bads, str_cols)
        df["county"] = df["County"].str.title()

        # Rename appropriate columns
        crename = {
            "Bed Census": CMU(
                category="hospital_beds_in_use",
                measurement="current",
                unit="beds"
            ),
            "Available": CMU(
                category="hospital_beds_available",
                measurement="current",
                unit="beds"
            ),
            "Total Staffed Bed Capacity": CMU(
                category="hospital_beds_capacity",
                measurement="current",
                unit="beds"
            )
        }

        # Drop grand total and melt
        out = df.query(
            "county != 'Grand Total'"
        ).melt(
            id_vars=["county"], value_vars=crename.keys()
        ).dropna()
        out["value"] = pd.to_numeric(out["value"])
        out = out.groupby(
            ["county", "variable"]
        ).sum().reset_index()

        # Extract category information and add other context
        out = self.extract_cat_measurement_unit(out, crename)

        out_cols = [
            "county", "category", "measurement", "unit",
            "age", "race", "sex", "value"
        ]

        return out.loc[:, out_cols]


class FloridaICUUsage(FloridaHospitalUsage):

    url = "https://bi.ahca.myflorida.com/t/ABICC/views/Public/ICUBedsHospital?:isGuestRedirectFromVizportal=y&:embed=y"

    def _clean_df(self, df: pd.DataFrame) -> pd.DataFrame:
        bads = [r",", r"%", "nan"]
        str_cols = ["County", "FileNumber", "ProviderName"]
        df = self._clean_cols(df, bads, str_cols)
        df["county"] = df["County"].str.title()

        # Create new columns
        df["ICU Census"] = df["Adult ICU Census"] + df["Pediatric ICU Census"]
        df["ICU Capacity"] = (
            df["Total AdultICU Capacity"] + df["Total PediatricICU Capacity"]
        )
        df["Available ICU"] = df["Available Adult ICU"] + df["Available Pediatric ICU"]

        # Rename appropriate columns
        crename = {
            "Adult ICU Census": CMU(
                category="adult_icu_beds_in_use",
                measurement="current",
                unit="beds"
            ),
            "Available Adult ICU": CMU(
                category="adult_icu_beds_available",
                measurement="current",
                unit="beds"
            ),
            "Total AdultICU Capacity": CMU(
                category="adult_icu_beds_capacity",
                measurement="current",
                unit="beds"
            ),
            "Pediatric ICU Census": CMU(
                category="pediatric_icu_beds_in_use",
                measurement="current",
                unit="beds"
            ),
            "Available Pediatric ICU": CMU(
                category="pediatric_icu_beds_available",
                measurement="current",
                unit="beds"
            ),
            "Total PediatricICU Capacity": CMU(
                category="pediatric_icu_beds_capacity",
                measurement="current",
                unit="beds"
            ),
            "ICU Census": CMU(
                category="icu_beds_in_use",
                measurement="current",
                unit="beds"
            ),
            "ICU Capacity": CMU(
                category="icu_beds_capacity",
                measurement="current",
                unit="beds"
            ),
            "Available ICU": CMU(
                category="icu_beds_available",
                measurement="current",
                unit="beds"
            ),
        }

        # Drop grand total and melt
        out = df.query(
            "county != 'Grand Total'"
        ).melt(
            id_vars=["county"], value_vars=crename.keys()
        ).dropna()
        out["value"] = pd.to_numeric(out["value"])
        out = out.groupby(
            ["county", "variable"]
        ).sum().reset_index()

        # Extract category information and add other context
        out = self.extract_cat_measurement_unit(out, crename)

        out_cols = [
            "county", "category", "measurement", "unit",
            "age", "race", "sex", "value"
        ]

        return out.loc[:, out_cols]


class FloridaHospitalCovid(TableauNeedsClick):
    url = "https://bi.ahca.myflorida.com/t/ABICC/views/Public/COVIDHospitalizationsCounty?:isGuestRedirectFromVizportal=y&:embed=y"

    def _clean_df(self, df: pd.DataFrame) -> pd.DataFrame:
        # Clean up column names
        df.columns = ["county", "value"]
        df["county"] = df["county"].str.title()
        df = df.query("county != 'Grand Total'")

        # Covnert to numeric
        df["value"] = pd.to_numeric(df["value"].astype(str).str.replace(",", ""))

        # Set category/measurment/unit/age/sex/race
        df.loc[:, "category"] = "hospital_beds_in_use_covid"
        df.loc[:, "measurement"] = "current"
        df.loc[:, "unit"] = "beds"
        df.loc[:, "age"] = "all"
        df.loc[:, "sex"] = "all"
        df.loc[:, "race"] = "all"

        return df


class FloridaHospital(DatasetBaseNoDate, CountyData):

    source = "https://bi.ahca.myflorida.com/t/ABICC/views/Public/HospitalBedsCounty"
    has_location = False
    state_fips = int(us.states.lookup("Florida").fips)

    def get(self):
        fiu = FloridaICUUsage()
        fhu = FloridaHospitalUsage()
        fhc = FloridaHospitalCovid()
        today = self._retrieve_dt("US/Eastern")
        vintage = self._retrieve_vintage()

        return pd.concat(
            [fiu.get(), fhu.get(), fhc.get()],
            ignore_index=True, sort=True, axis=0
        ).assign(dt=today, vintage=vintage)


class FLCounty(DatasetBaseNoDate, ArcGIS):
    ARCGIS_ID = "CY1LXxl9zlJeBuRZ"
    state_fips = int(us.states.lookup("Florida").fips)
    has_location = True
    source = "https://experience.arcgis.com/experience/96dd742462124fa0b38ddedb9b25e429"

    def get(self):
        df = self.get_all_sheet_to_df("Florida_COVID19_Cases", 0, 1)
        df.columns = [x.lower() for x in list(df)]
        df["location"] = (self.state_fips*1000) + df["county"].astype(int)

        # 12025 is the OLD (retired in 1997) fips code for Date county. It is now known
        # as Miami-Dade county with fips code 12086
        df.loc[:, "location"] = df["location"].replace(12025, 12086)

        crename = {
            "casesall": CMU(
                category="cases",
                measurement="cumulative",
                unit="people"
            ),
            "deaths": CMU(
                category="deaths",
                measurement="cumulative",
                unit="people"
            ),
            "newpos": CMU(
                category="unspecified_tests_positive",
                measurement="new",
                unit="test_encounters"
            ),
            "newneg": CMU(
                category="unspecified_tests_negative",
                measurement="new",
                unit="test_encounters"
            ),
            "newtested": CMU(
                category="unspecified_tests_total",
                measurement="new",
                unit="test_encounters"
            ),
        }
        out = df.melt(
            id_vars=["location"], value_vars=crename.keys()
        ).assign(
            dt=self._retrieve_dt("US/Eastern"), vintage=self._retrieve_vintage()
        ).query(
            "location not in (12998, 12999)"
        ).dropna()
        out.loc[:, "value"] = pd.to_numeric(out["value"])

        # Extract category information and add other variable context
        out = self.extract_cat_measurement_unit(out, crename)

        cols_to_keep = [
            "vintage", "dt", "location", "category", "measurement", "unit",
            "age", "race", "sex", "value"
        ]

        return out.loc[:, cols_to_keep]
