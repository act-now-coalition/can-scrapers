import pandas as pd
import us
import datetime as dt
from can_tools.scrapers.base import CMU

import requests
import os

from can_tools.scrapers import variables
from can_tools.scrapers.official.base import TableauDashboard

class NCVaccineAge(TableauDashboard):
    has_location = False
    source = "https://covid19.ncdhhs.gov/dashboard/vaccinations"
    source_name = (
        "North Carolina Department of Health and Human Services Covid-19 Response"
    )
    state_fips = int(us.states.lookup("North Carolina").fips)
    location_type = "county"
    baseurl = "https://public.tableau.com"
    viewPath = "NCDHHS_COVID-19_Dashboard_Vaccinations/Demographics"
    timezone = "US/Eastern"
    filterFunctionName = "[Parameters].[Parameter 3 1]" # county 
    secondaryFilterFunctionName = "[Parameters].[Param.DemographicMetric (copy)_1353894643018190853]" # dose type

    # map wide form column names into CMUs
    variables = {
        " Population Vaccinated with at Least One Dose": variables.PERCENTAGE_PEOPLE_INITIATING_VACCINE,
        " Population Fully Vaccinated": variables.PERCENTAGE_PEOPLE_COMPLETING_VACCINE,
    }

    worksheet = "Age_Percent_Pop_County"
    demo_col = "age"
    demo_rename = "Age Group-alias"
    demo_replacements = {
        "Missing or Undisclosed": "unknown",
        "75+": "75_plus",
        "0-17 (16-17)": "0-17",
    }
    all_demo_cols = ["age", "race", "ethnicity", "sex"]

    def fetch(self):
        path = os.path.dirname(__file__) + "/../../../bootstrap_data/locations.csv"
        counties = list(
            pd.read_csv(path).query(f"state == 37 and location != 37")["name"]
        )

        dfs = []
        # get each county
        for county in counties:
            print('working on: ', county)
            self.filterFunctionValue = county + " County"   
            # get both initiated and completed vals
            for dose_val in ['3', '4']:
                print("dose key: ", dose_val)
                self.secondaryFilterValue = dose_val
                df = self.get_tableau_view()[self.worksheet]
                dfs.append(df.assign(location_name=county))
        
        return pd.concat(dfs)
        
    def normalize(self, data: pd.DataFrame) -> pd.DataFrame:
        df = (
            data.rename(columns={self.demo_rename:self.demo_col, 'AGG(calc.RunningSum.DemographicMetric)-alias':'value', 'ATTR(text.tooltips)-alias':'variable', 'Week of-value':'dt'})
            .loc[:, ['location_name', 'dt', 'variable', 'value', self.demo_col]]
            .query(f"variable != '%missing%' and {self.demo_col} != 'Suppressed'")
            .replace(self.demo_replacements)
            .assign(
                value=lambda x: pd.to_numeric(x['value'].astype(str)) * 100,
                vintage=self._retrieve_vintage(),
            )
            .pipe(self.extract_CMU, self.variables, skip_columns=[self.demo_col])
        )
        return df.drop(columns={'variable'})

class NCVaccineRace(NCVaccineAge):
    worksheet = "Race_Percent_Pop_County"
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
    worksheet = "Gender_Percent_Pop_county"
    demo_col = "sex"
    demo_rename = "Gender-alias"
    demo_replacements = {
        "Missing or Undisclosed": "unknown",
        "Male": "male",
        "Female": "female",
    }


class NCVaccineEthnicity(NCVaccineAge):
    worksheet = "Ethnicity_Percent_Pop_county"
    demo_col = "ethnicity"
    demo_rename = "Ethnicity-alias"
    demo_replacements = {
        "Missing or Undisclosed": "unknown",
        "Hispanic": "hispanic",
        "Non-Hispanic": "non-hispanic",
    }
