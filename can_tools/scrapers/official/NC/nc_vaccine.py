import pandas as pd
from pandas.core.frame import DataFrame
import us
import datetime as dt
from can_tools.scrapers.base import CMU
from can_tools.scrapers.official.base import TableauDashboard
from tableauscraper import TableauScraper as TS


class NCVaccine(TableauDashboard):
    has_location = False
    source = "https://covid19.ncdhhs.gov/dashboard/vaccinations"
    source_name = (
        "North Carolina Department of Health and Human Services Covid-19 Response"
    )
    state_fips = int(us.states.lookup("North Carolina").fips)
    location_type = "county"
    baseurl = "https://public.tableau.com"
    viewPath = "NCDHHS_COVID-19_Dashboard_Vaccinations/Summary"
    location_name = "NorthCarolina"
    data_tableau_table = "County Map"
    location_name_col = "County -alias"
    timezone = "US/Eastern"

    # map wide form column names into CMUs
    cmus = {
        "SUM(Partially Vaccinated)-alias": CMU(
            category="total_vaccine_initiated",
            measurement="cumulative",
            unit="people",
        ),
        "SUM(Fully Vaccinated)-alias": CMU(
            category="total_vaccine_completed",
            measurement="cumulative",
            unit="people",
        ),
    }

    def fetch(self):
        """
        uses the tableauscraper module:
        https://github.com/bertrandmartel/tableau-scraping/blob/master/README.md
        """
        ts = TS()
        ts.loads("https://public.tableau.com/views/NCDHHS_COVID-19_Dashboard_Vaccinations/Demographics")
        workbook = ts.getWorkbook()

        ageWeekly = workbook.getWorksheet("Age_Weekly_Statewide")
        raceWeekly = workbook.getWorksheet("Race Weekly Statewide")
        genderWeekly = workbook.getWorksheet("Gender_Weekly_statewide")
        ethnicityWeekly = workbook.getWorksheet("Ethnicity_Weekly_Statewide")

        frames = [ageWeekly.data, raceWeekly.data, genderWeekly.data, ethnicityWeekly.data]

        return frames

    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        timeran = dt.datetime.now()

        age_df = df[0]

        age_df.rename(columns = {
            "Week of-value" : "dt",
            "Age Group (copy)-alias" : "age",
            "AGG(Calc.Demographic Count Metric)-alias" : "value"
        }, inplace=True)

        age_df = age_df.replace("Missing or Undisclosed", "unknown")
        age_df = age_df.replace("75+", "75_plus")
        age_df = age_df.replace("0-17 (16-17)", "0-17")
        age_df["vintage"] = pd.Series([timeran] * len(age_df))
        age_df["category"] = "total_vaccine_initiated"
        age_df["measurement"] = "new"
        age_df["unit"] = "percentage"
        age_df["value"] = pd.to_numeric(age_df["value"])
        age_df["value"] = age_df["value"].apply(lambda x: x*100)
        age_df["race"] = "all"
        age_df["sex"] = "all"
        age_df["ethnicity"] = "all"
        age_df = age_df[["vintage", "dt", "category", "measurement", "unit", "age", "race", "ethnicity", "sex", "value"]]


        race_df = df[1]
        race_df.rename(columns = {
            "Week of-value" : "dt",
            "AGG(Calc.Demographic Count Metric)-alias" : "value",
            "Race-alias" : "race"
        }, inplace=True)
        race_df = race_df.replace("Missing or Undisclosed", "unknown")
        race_df = race_df.replace("Other", "other")
        race_df = race_df.replace("Black or African American", "black")
        race_df = race_df.replace("White", "white")
        race_df = race_df.replace("Asian or Pacific Islander", "asian")
        race_df = race_df.replace("American Indian or Alaskan Native", "ai_an")
        race_df["vintage"] = pd.Series([timeran] * len(race_df))
        race_df["category"] = "total_vaccine_initiated"
        race_df["measurement"] = "new"
        race_df["unit"] = "percentage"
        race_df["ethnicity"] = "all"
        race_df["sex"] = "all"
        race_df["age"] = "all"
        race_df = race_df[["vintage", "dt", "category", "measurement", "unit", "age", "race", "ethnicity", "sex", "value"]]


        gender_df = df[2]
        gender_df.rename(columns = {
            "Week of-value" : "dt",
            "AGG(Calc.Demographic Count Metric)-alias" : "value",
            "Gender-alias" : "sex"
        }, inplace=True)
        gender_df = gender_df.replace("Missing or Undisclosed", "unknown")
        gender_df = gender_df.replace("Male", "male")
        gender_df = gender_df.replace("Female", "female")
        gender_df["vintage"] = pd.Series([timeran] * len(gender_df))
        gender_df["category"] = "total_vaccine_initiated"
        gender_df["measurement"] = "new"
        gender_df["unit"] = "percentage"
        gender_df["ethnicity"] = "all"
        gender_df["age"] = "all"
        gender_df["race"] = "all"
        gender_df = gender_df[["vintage", "dt", "category", "measurement", "unit", "age", "race", "ethnicity", "sex", "value"]]


        ethnicity_df = df[3]        
        ethnicity_df.rename(columns = {
            "WEEK(Week of)-value" : "dt",
            "AGG(Calc.Demographic Count Metric)-alias" : "value",
            "Ethnicity-alias" : "ethnicity"
        }, inplace=True)
        ethnicity_df = ethnicity_df.replace("Missing or Undisclosed", "unknown")
        ethnicity_df = ethnicity_df.replace("Non-Hispanic", "non-hispanic")
        ethnicity_df = ethnicity_df.replace("Hispanic", "hispanic")
        ethnicity_df["vintage"] = pd.Series([timeran] * len(age_df))
        ethnicity_df["category"] = "total_vaccine_initiated"
        ethnicity_df["measurement"] = "new"
        ethnicity_df["unit"] = "percentage"
        ethnicity_df["sex"] = "all"
        ethnicity_df["age"] = "all"
        ethnicity_df["race"] = "all"
        ethnicity_df = ethnicity_df[["vintage", "dt", "category", "measurement", "unit", "age", "race", "ethnicity", "sex", "value"]]

        out_df = pd.concat([age_df, race_df, gender_df, ethnicity_df], ignore_index=True)
        out_df['location_name'] = self.location_name

        return out_df
        