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
    demo_col = "age"
    demo_rename = "Age Group (copy)-alias"
    demo_replacements = {
        "Missing or Undisclosed": "unknown",
        "75+": "75_plus",
        "0-17 (16-17)": "0-17",
    }
    all_demo_cols = ["age", "race", "ethnicity", "sex"]
    variables = {"first_shot": variables.INITIATING_VACCINATIONS_ALL}

    def fetch(self) -> pd.DataFrame:
        ts = TS()
        ts.loads(
            "https://public.tableau.com/views/NCDHHS_COVID-19_Dashboard_Vaccinations/Demographics"
        )
        workbook = ts.getWorkbook()
        return workbook.getWorksheet(self.worksheet).data

    def normalize(self, data: pd.DataFrame) -> pd.DataFrame:
        df = data.rename(
            columns={
                "Week of-value": "dt",
                "AGG(Calc.Demographic Count Metric)-alias": "first_shot",
                self.demo_rename: self.demo_col,
            }
        ).assign(location=self.state_fips)
        df[self.demo_col] = df[self.demo_col].replace(self.demo_replacements)
        out = self._reshape_variables(
            df,
            id_vars=[self.demo_col],
            variable_map=self.variables,
            skip_columns=[self.demo_col],
        )
        return out


class NCVaccineRace(NCVaccineAge):

    worksheet = "Race Weekly Statewide"
    demo_col = "race"
    demo_rename = "Race-alias"
    demo_replacements = {
        "Missing or Undisclosed": "unknown",
        "Other": "other",
        "Black or African American": "black",
        "White": "white",
        "Asian or Pacific Islander": "asian",
        "American Indian or Alaskan Native": "ai_an",
    }


class NCVaccineSex(NCVaccineAge):

    worksheet = "Gender_Weekly_statewide"
    demo_col = "sex"
    demo_rename = "Gender-alias"
    demo_replacements = {
        "Missing or Undisclosed": "unknown",
        "Male": "male",
        "Female": "female",
    }


class NCVaccineEthnicity(NCVaccineAge):
    worksheet = "Ethnicity_Weekly_Statewide"
    demo_col = "ethnicity"
    demo_rename = "Ethnicity-alias"
    demo_replacements = {
        "Missing or Undisclosed": "unknown",
        "Hispanic": "hispanic",
        "Non-Hispanic": "non-hispanic",
    }
