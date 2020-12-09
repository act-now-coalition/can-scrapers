from abc import ABC

import pandas as pd

import pyppeteer
import us
from typing import List

from can_tools.scrapers.base import CMU
from can_tools.scrapers.official.base import StateDashboard
from can_tools.scrapers.puppet import TableauNeedsClick


class MissouriBase(
    StateDashboard,
    TableauNeedsClick,
    ABC,
):
    source = "https://results.mo.gov/t/COVID19/views/COVID-19DataforDownload/{tab}?:isGuestRedirectFromVizportal=y&:embed=y"
    has_location = False
    location_type = "county"
    state_fips = int(us.states.lookup("Missouri").fips)

    out_cols = [
        "dt",
        "vintage",
        "location_name",
        "category",
        "measurement",
        "unit",
        "age",
        "race",
        "sex",
        "value",
    ]

    # def clean_desoto(self, df: pd.DataFrame):
    #    df.loc[df["location_name"] == "Desoto", "location_name"] = "DeSoto"


async def _special_tableau_handler(page, tmpdirname):
    # Get a reference to the page that was just opened
    pages = await page.browser.pages()
    popup = pages[-1]

    import requests
    import os

    await popup.waitForSelector(".csvLink_summary")
    url = await popup.querySelectorEval(".csvLink_summary", "node => node.href")
    data = requests.get(url)
    open(
        os.path.join(tmpdirname, "Metrics_by_Test_Date_by_County_data.csv"), "wb"
    ).write(data.content)


class MissouriState(MissouriBase):
    """
    Fetch details about overall Missouri state testing by county
    """

    headless = True
    url = MissouriBase.source.format(tab="MetricsbyTestDatebyCounty")

    # TODO0: ZW: The Data tab below is better... I think. Remove this once confirmed which data to use
    #    # Download Crosstab from Tableau
    #    tableau_click_selector_list: List[str] = [
    #        "@data-tb-test-id = 'DownloadCrosstab-Button'",
    #        "@data-tb-test-id = 'crosstab-options-dialog-radio-csv-Label'",
    #        "@data-tb-test-id = 'export-crosstab-export-Button'"
    #    ]

    # Download Data from Tableau
    tableau_click_selector_list: List[str] = [
        "@data-tb-test-id = 'DownloadData-Button'",
        _special_tableau_handler,
    ]

    async def _pre_click(self, page: pyppeteer.page.Page):
        # TODO0: ZW: This page is slow to load. Adding wait.
        #  Should probably find a better selector to use to determine that the page has loaded
        import asyncio

        await asyncio.sleep(5)
        await page.waitForSelector("#main-content")

    def _clean_cols(self, df: pd.DataFrame, bads: list, str_cols: list) -> pd.DataFrame:
        for col in list(df):
            for bad in bads:
                df[col] = df[col].astype(str).str.replace(bad, "")
            if col not in str_cols:
                df[col] = pd.to_numeric(df[col])

        return df

    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        bads = [r",", r"%", "nan"]
        str_cols = ["County", "FileNumber", "ProviderName"]
        df = self._clean_cols(df, bads, str_cols)
        df["location_name"] = df["County"].str.title()

        # Rename appropriate columns
        crename = {
            "Bed Census": CMU(
                category="hospital_beds_in_use", measurement="current", unit="beds"
            ),
            "Available": CMU(
                category="hospital_beds_available", measurement="current", unit="beds"
            ),
            "Total Staffed Bed Capacity": CMU(
                category="hospital_beds_capacity", measurement="current", unit="beds"
            ),
        }

        # Drop grand total and melt
        out = (
            df.query("location_name != 'Grand Total'")
            .melt(id_vars=["location_name"], value_vars=crename.keys())
            .dropna()
        )
        out["value"] = pd.to_numeric(out["value"])
        out = out.groupby(["location_name", "variable"]).sum().reset_index()

        # Extract category information and add other context
        out = self.extract_CMU(out, crename)
        out["dt"] = self._retrieve_dt("US/Eastern")
        out["vintage"] = self._retrieve_vintage()
        # self.clean_desoto(out)
        return out.loc[:, self.out_cols]


# class MissouriICUUsage(MissouriHospitalUsage):
#    """
#    Fetch ICU usage details from Tableau dashboard
#    """

#    url = "https://bi.ahca.myflorida.com/t/ABICC/views/Public/ICUBedsHospital?:isGuestRedirectFromVizportal=y&:embed=y"

#    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
#        bads = [r",", r"%", "nan"]
#        str_cols = ["County", "FileNumber", "ProviderName"]
#        df = self._clean_cols(df, bads, str_cols)
#        df["location_name"] = df["County"].str.title()

#        # Create new columns
#        df["ICU Census"] = df["Adult ICU Census"] + df["Pediatric ICU Census"]
#        df["ICU Capacity"] = (
#            df["Total AdultICU Capacity"] + df["Total PediatricICU Capacity"]
#        )
#        df["Available ICU"] = df["Available Adult ICU"] + df["Available Pediatric ICU"]

#        # Rename appropriate columns
#        crename = {
#            "Adult ICU Census": CMU(
#                category="adult_icu_beds_in_use", measurement="current", unit="beds"
#            ),
#            "Available Adult ICU": CMU(
#                category="adult_icu_beds_available", measurement="current", unit="beds"
#            ),
#            "Total AdultICU Capacity": CMU(
#                category="adult_icu_beds_capacity", measurement="current", unit="beds"
#            ),
#            "Pediatric ICU Census": CMU(
#                category="pediatric_icu_beds_in_use", measurement="current", unit="beds"
#            ),
#            "Available Pediatric ICU": CMU(
#                category="pediatric_icu_beds_available",
#                measurement="current",
#                unit="beds",
#            ),
#            "Total PediatricICU Capacity": CMU(
#                category="pediatric_icu_beds_capacity",
#                measurement="current",
#                unit="beds",
#            ),
#            "ICU Census": CMU(
#                category="icu_beds_in_use", measurement="current", unit="beds"
#            ),
#            "ICU Capacity": CMU(
#                category="icu_beds_capacity", measurement="current", unit="beds"
#            ),
#            "Available ICU": CMU(
#                category="icu_beds_available", measurement="current", unit="beds"
#            ),
#        }

#        # Drop grand total and melt
#        out = (
#            df.query("location_name != 'Grand Total'")
#            .melt(id_vars=["location_name"], value_vars=crename.keys())
#            .dropna()
#        )
#        out["value"] = pd.to_numeric(out["value"])
#        out = out.groupby(["location_name", "variable"]).sum().reset_index()
#        out.loc[out["location_name"] == "Desoto", "location_name"] = "DeSoto"

#        # Extract category information and add other context
#        out = self.extract_CMU(out, crename)
#        out["dt"] = self._retrieve_dt("US/Eastern")
#        out["vintage"] = self._retrieve_vintage()
#        self.clean_desoto(out)
#        return out.loc[:, self.out_cols]


# class MissouriHospitalCovid(MissouriHospitalBase):
#    """
#    Fetch count of all hospital beds in use by covid patients
#    """

#    url = "https://bi.ahca.myflorida.com/t/ABICC/views/Public/COVIDHospitalizationsCounty?:isGuestRedirectFromVizportal=y&:embed=y"

#    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
#        # Clean up column names
#        df.columns = ["location_name", "value"]
#        df["location_name"] = df["location_name"].str.title()
#        df = df.query("location_name != 'Grand Total'")

#        # Covnert to numeric
#        df["value"] = pd.to_numeric(df["value"].astype(str).str.replace(",", ""))

#        # Set category/measurment/unit/age/sex/race
#        df["category"] = "hospital_beds_in_use_covid"
#        df["measurement"] = "current"
#        df["unit"] = "beds"
#        df["age"] = "all"
#        df["sex"] = "all"
#        df["race"] = "all"
#        df["dt"] = self._retrieve_dt("US/Eastern")
#        df["vintage"] = self._retrieve_vintage()

#        self.clean_desoto(df)

#        return df
