import pandas as pd
import us
import datetime as dt
from can_tools.scrapers.base import CMU

from can_tools.scrapers import variables
from can_tools.scrapers.official.base import TableauDashboard
from tableauscraper import TableauScraper as TS


class NCVaccine(TableauDashboard):
    has_location = False
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
        "AGG(Calc.Tooltip Partially Vaccinated)-alias": variables.INITIATING_VACCINATIONS_ALL,
        "AGG(Calc.Tooltip Fully Vaccinated)-alias": variables.FULLY_VACCINATED_ALL,
    }

    def fetch(self):
        """
        uses the tableauscraper module:
        https://github.com/bertrandmartel/tableau-scraping/blob/master/README.md
        """
        ts = TS()
        ts.loads(
            "https://public.tableau.com/views/NCDHHS_COVID-19_Dashboard_Vaccinations/Demographics"
        )
        workbook = ts.getWorkbook()

        ageWeekly = workbook.getWorksheet("Age_Weekly_Statewide")
        raceWeekly = workbook.getWorksheet("Race Weekly Statewide")
        genderWeekly = workbook.getWorksheet("Gender_Weekly_statewide")
        ethnicityWeekly = workbook.getWorksheet("Ethnicity_Weekly_Statewide")

        frames = [
            ageWeekly.data,
            raceWeekly.data,
            genderWeekly.data,
            ethnicityWeekly.data,
        ]

        return frames

    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        self.timeran = dt.datetime.now()

        age_df = self._normalize_age(df[0])
        race_df = self._normalize_race(df[1])
        gender_df = self._normalize_gender(df[2])     
        ethnicity_df = self._normalize_ethnicity(df[3])

        out_df = pd.concat(
            [age_df, race_df, gender_df, ethnicity_df], ignore_index=True
        )
        out_df["location_name"] = "North Carolina"

        return out_df

    def _normalize_age(self, df: pd.DataFrame) -> pd.DataFrame:
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
        age_df["vintage"] = pd.Series([self.timeran] * len(age_df))
        age_df["category"] = "total_vaccine_initiated"
        age_df["measurement"] = "new"
        age_df["unit"] = "percentage"
        age_df["value"] = pd.to_numeric(age_df["value"])
        age_df["value"] = age_df["value"].apply(lambda x: x * 100)
        age_df["race"] = "all"
        age_df["sex"] = "all"
        age_df["ethnicity"] = "all"
        age_df = age_df[
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
        ]

        return age_df

    def _normalize_race(self, df: pd.DataFrame) -> pd.DataFrame:
        race_df = df
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
        race_df["vintage"] = pd.Series([self.timeran] * len(race_df))
        race_df["category"] = "total_vaccine_initiated"
        race_df["measurement"] = "new"
        race_df["unit"] = "percentage"
        race_df["ethnicity"] = "all"
        race_df["sex"] = "all"
        race_df["age"] = "all"
        race_df = race_df[
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
        ]

        return race_df

    def _normalize_gender(self, df: pd.DataFrame) -> pd.DataFrame:
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
        gender_df["vintage"] = pd.Series([self.timeran] * len(gender_df))
        gender_df["category"] = "total_vaccine_initiated"
        gender_df["measurement"] = "new"
        gender_df["unit"] = "percentage"
        gender_df["ethnicity"] = "all"
        gender_df["age"] = "all"
        gender_df["race"] = "all"
        gender_df = gender_df[
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
        ]

        return gender_df

    def _normalize_ethnicity(self, df: pd.DataFrame) -> pd.DataFrame:
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
        ethnicity_df["vintage"] = pd.Series([self.timeran] * len(ethnicity_df))
        ethnicity_df["category"] = "total_vaccine_initiated"
        ethnicity_df["measurement"] = "new"
        ethnicity_df["unit"] = "percentage"
        ethnicity_df["sex"] = "all"
        ethnicity_df["age"] = "all"
        ethnicity_df["race"] = "all"
        ethnicity_df = ethnicity_df[
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
        ]

        return ethnicity_df