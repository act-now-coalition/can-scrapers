import pandas as pd
import us
import datetime as dt
from can_tools.scrapers.base import CMU

from can_tools.scrapers import variables
from can_tools.scrapers.official.base import TableauDashboard
from tableauscraper import TableauScraper as TS


class NCVaccineAge(TableauDashboard):
    has_location = True
    source = "https://covid19.ncdhhs.gov/dashboard/vaccinations"
    source_name = (
        "North Carolina Department of Health and Human Services Covid-19 Response"
    )
    state_fips = int(us.states.lookup("North Carolina").fips)
    location_type = "state"
    baseurl = "https://public.tableau.com"
    viewPath = "NCDHHS_COVID-19_Dashboard_Vaccinations/Summary"
    data_tableau_table = "County Map"
    location_name_col = "County -alias"
    timezone = "US/Eastern"

    # map wide form column names into CMUs
    cmus = {
        "AGG(Calc.Tooltip At Least One Dose Vaccinated)-alias": variables.INITIATING_VACCINATIONS_ALL,
        "AGG(Calc.Tooltip Fully Vaccinated)-alias": variables.FULLY_VACCINATED_ALL,
    }

    # for this scraper
    worksheet = "Age_Weekly_Statewide"

    def fetch(self) -> pd.DataFrame:
        ts = TS()
        ts.loads(
            "https://public.tableau.com/views/NCDHHS_COVID-19_Dashboard_Vaccinations/Demographics"
        )
        workbook = ts.getWorkbook()
        return workbook

        return workbook.getWorksheet(self.worksheet).data

    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        age_df = df
        age_df.rename(
            columns={
                "Week of-value": "dt",
                "Age Group (copy)-alias": "age",
                "AGG(Calc.Demographic Count Metric)-alias": "value",
            },
            inplace=True,
        )

        age_df = age_df.replace("Missing or Undisclosed", "unknown")
        age_df = age_df.replace("75+", "75_plus")
        age_df = age_df.replace("0-17 (16-17)", "0-17")
        age_df["vintage"] = self._retrieve_vintage()
        age_df["category"] = "total_vaccine_initiated"
        age_df["measurement"] = "new"
        age_df["unit"] = "percentage"
        age_df["value"] = pd.to_numeric(age_df["value"])
        age_df["value"] = age_df["value"].apply(lambda x: x * 100)
        age_df["race"] = "all"
        age_df["sex"] = "all"
        age_df["ethnicity"] = "all"
        return age_df[
            [
                "vintage",
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
        ].assign(location=self.state_fips)


class NCVaccineRace(NCVaccineAge):

    worksheet = "Race Weekly Statewide"

    def normalize(self, data: pd.DataFrame) -> pd.DataFrame:
        race_df = data
        race_df.rename(
            columns={
                "Week of-value": "dt",
                "AGG(Calc.Demographic Count Metric)-alias": "value",
                "Race-alias": "race",
            },
            inplace=True,
        )

        race_df = race_df.replace("Missing or Undisclosed", "unknown")
        race_df = race_df.replace("Other", "other")
        race_df = race_df.replace("Black or African American", "black")
        race_df = race_df.replace("White", "white")
        race_df = race_df.replace("Asian or Pacific Islander", "asian")
        race_df = race_df.replace("American Indian or Alaskan Native", "ai_an")
        race_df["vintage"] = self._retrieve_vintage()
        race_df["category"] = "total_vaccine_initiated"
        race_df["measurement"] = "new"
        race_df["unit"] = "percentage"
        race_df["ethnicity"] = "all"
        race_df["sex"] = "all"
        race_df["age"] = "all"
        return race_df[
            [
                "vintage",
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
        ].assign(location=self.state_fips)


class NCVaccineSex(NCVaccineAge):

    worksheet = "Gender_Weekly_statewide"

    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        gender_df = df
        gender_df.rename(
            columns={
                "Week of-value": "dt",
                "AGG(Calc.Demographic Count Metric)-alias": "value",
                "Gender-alias": "sex",
            },
            inplace=True,
        )

        gender_df = gender_df.replace("Missing or Undisclosed", "unknown")
        gender_df = gender_df.replace("Male", "male")
        gender_df = gender_df.replace("Female", "female")
        gender_df["vintage"] = self._retrieve_vintage()
        gender_df["category"] = "total_vaccine_initiated"
        gender_df["measurement"] = "new"
        gender_df["unit"] = "percentage"
        gender_df["ethnicity"] = "all"
        gender_df["age"] = "all"
        gender_df["race"] = "all"
        return gender_df[
            [
                "vintage",
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
        ].assign(location=self.state_fips)


class NCVaccineEthnicity(NCVaccineAge):
    worksheet = "Ethnicity_Weekly_Statewide"

    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        ethnicity_df = df
        ethnicity_df.rename(
            columns={
                "WEEK(Week of)-value": "dt",
                "AGG(Calc.Demographic Count Metric)-alias": "value",
                "Ethnicity-alias": "ethnicity",
            },
            inplace=True,
        )

        ethnicity_df = ethnicity_df.replace("Missing or Undisclosed", "unknown")
        ethnicity_df = ethnicity_df.replace("Non-Hispanic", "non-hispanic")
        ethnicity_df = ethnicity_df.replace("Hispanic", "hispanic")
        ethnicity_df["vintage"] = self._retrieve_vintage()
        ethnicity_df["category"] = "total_vaccine_initiated"
        ethnicity_df["measurement"] = "new"
        ethnicity_df["unit"] = "percentage"
        ethnicity_df["sex"] = "all"
        ethnicity_df["age"] = "all"
        ethnicity_df["race"] = "all"
        return ethnicity_df[
            [
                "vintage",
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
        ].assign(location=self.state_fips)
