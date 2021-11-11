
import pandas as pd
import us
import os
import multiprocessing as mp
from tableauscraper import TableauScraper

from can_tools.scrapers import variables
from can_tools.scrapers import NCVaccine

class NCVaccineAge(NCVaccine):
    viewPath = "NCDHHS_COVID-19_Dashboard_Vaccinations/VaccinationDashboard"
    timezone = "US/Eastern"
    # filterFunctionName = "[Parameters].[Parameter 3 1]"  # county
    # secondaryFilterFunctionName = (
        # "[Parameters].[Param.DemographicMetric (copy)_1353894643018190853]"  # dose type
    # )
    # thirdFilterFunctionName = "[Parameters].[Parameter 2]"  # specify providers
    # thirdFilterFunctionValue = "3"
    baseurl = "https://public.tableau.com"

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

    def fetch(self):
        engine = TableauScraper()
        engine.loads("https://public.tableau.com/views/NCDHHS_COVID-19_Dashboard_Vaccinations/VaccinationDashboard")
        return engine.getWorkbook()

        # counties = self._retrieve_counties()
        # return self._get_data(counties)




    def _get_data(self, counties):
        data = []
        for county in counties:
            # self.filterFunctionValue = county + " County"
            # for dose_val in ["3", "4"]:
                # print("working on:", county, "dose: ", dose_val)
                # self.secondaryFilterValue = dose_val
                df = self.get_tableau_view()[self.worksheet]
                data.append(df.assign(location_name=county))
        return data

    def normalize(self, data: pd.DataFrame) -> pd.DataFrame:
        df = (
            data.rename(
                columns={
                    self.demo_rename: self.demo_col,
                    "AGG(calc.RunningSum.DemographicMetric)-alias": "value",
                    "ATTR(text.tooltips)-alias": "variable",
                    "Week of-value": "dt",
                }
            )
            .loc[:, ["location_name", "dt", "variable", "value", self.demo_col]]
            .query(f"variable != '%missing%' and {self.demo_col} != 'Suppressed'")
            .replace(self.demo_replacements)
            .assign(
                value=lambda x: pd.to_numeric(x["value"].astype(str)) * 100,
                vintage=self._retrieve_vintage(),
            )
            .pipe(self.extract_CMU, self.variables, skip_columns=[self.demo_col])
        )
        return df.drop(columns={"variable"})