from abc import ABC
import multiprocessing
import pandas as pd
import us
from typing import Dict

from can_tools.scrapers import variables
from can_tools.scrapers.official.base import TableauDashboard
from tableauscraper import TableauScraper, TableauWorkbook


class NCVaccineBase(TableauDashboard, ABC):
    has_location = False
    source = "https://covid19.ncdhhs.gov/dashboard/vaccinations"
    source_name = (
        "North Carolina Department of Health and Human Services Covid-19 Response"
    )
    state_fips = int(us.states.lookup("NC").fips)
    location_type = "county"
    fetch_url = (
        "https://public.tableau.com/views/NCDHHS_COVID-19_Dashboard_Vaccinations/All"
    )

    timezone = "US/Eastern"

    # map wide form column names into CMUs
    cmus = {
        "At Least One Dose": variables.INITIATING_VACCINATIONS_ALL,
        "Initial Series Complete": variables.FULLY_VACCINATED_ALL,
        "At Least One Original Booster": variables.PEOPLE_VACCINATED_ADDITIONAL_DOSE,
    }

    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        df = (
            self._rename_or_add_date_and_location(
                df,
                location_name_column="location_name",
                timezone=self.timezone,
                location_names_to_replace={"Mcdowell": "McDowell"},
            )
            .rename(
                columns={
                    "SUM(metric_count)-alias": "value",
                    "calc. label colors-alias": "variable",
                }
            )
            .drop(
                columns=[
                    "calc. label colors-value",
                    "AGG(calc. 1 (labels))-value",
                    "AGG(calc. 1 (labels))-alias",
                    "ATTR(calc. tooltip na county)-alias",
                    "AGG(calc. percent labels)-alias",
                ]
            )
            .query("variable != 'Updated Booster'")
        )

        df["vintage"] = self._retrieve_vintage()
        df = self.extract_CMU(df, self.cmus)
        return df


class NCVaccineCounty(NCVaccineBase):
    def fetch(self):
        counties = self._retrieve_counties()
        pool = multiprocessing.Pool()
        dfs = pool.map(self._fetch_county, counties)
        return pd.concat(dfs)

    def _fetch_county(self, county: str) -> pd.DataFrame:
        scraper_instance = TableauScraper()
        scraper_instance.loads(self.fetch_url)
        workbook = scraper_instance.getWorkbook()

        full_county = f"{county} County"
        county_workbook = workbook.setParameter("county Parameter", full_county)
        init_completed = county_workbook.getWorksheet("VaxWaffle_Label_All").data
        booster = county_workbook.getWorksheet("BoosterWaffle_Label_All").data
        data = pd.concat([init_completed, booster])
        data["location_name"] = county
        return data


class NCVaccineState(NCVaccineBase):
    location_type = "state"
    has_location = False

    def fetch(self) -> TableauWorkbook:
        scraper_instance = TableauScraper()
        scraper_instance.loads(self.fetch_url)
        workbook = scraper_instance.getWorkbook()
        init_completed = workbook.getWorksheet("VaxWaffle_Label_All").data
        booster = workbook.getWorksheet("BoosterWaffle_Label_All").data
        data = pd.concat([init_completed, booster])
        data["location_name"] = "North Carolina"
        return data
