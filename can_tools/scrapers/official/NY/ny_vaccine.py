import us
import pandas as pd
import os

from can_tools.scrapers import variables
from can_tools.scrapers.official.base import TableauDashboard
from typing import List


class NewYorkVaccineCounty(TableauDashboard):
    has_location = False
    source = "https://covid19vaccine.health.ny.gov/covid-19-vaccine-tracker"
    source_name = "New York State Department of Health"
    state_fips = int(us.states.lookup("New York").fips)
    location_type = "county"
    baseurl = "https://public.tableau.com"
    viewPath = "Vaccine_County_Public/NYSCountyVaccinations"

    data_tableau_table = "Total Doses by County"
    location_name_col = "County-alias"
    timezone = "US/Eastern"

    cmus = {
        "First Dose": variables.INITIATING_VACCINATIONS_ALL,
        "Series Complete": variables.FULLY_VACCINATED_ALL,
    }

    def normalize(self, data: pd.DataFrame) -> pd.DataFrame:
        columns = {
            "Measure Names-alias": "variable",
            "County-value": "location_name",
            "Measure Values-alias": "value",
        }
        data = (
            data.loc[:, list(columns.keys())]
            .rename(columns=columns)
            .assign(
                dt=self._retrieve_dt(),
                vintage=self._retrieve_vintage(),
                value=lambda row: pd.to_numeric(row["value"].str.replace(",", "")),
            )
            .pipe(self.extract_CMU, cmu=self.cmus)
        )
        return data


class NewYorkVaccineCountyAge(NewYorkVaccineCounty):
    viewPath = "Gender_Age_Public/VaccinationbyAge"
    filterFunctionName = (
        "[federated.1nz68qa0ypytxa16suf0a0hhpoyr].[none:PAT_ZIP_COUNTY_DESC:nk]"
    )
    secondaryFilterFunctionName = "[Parameters].[Parameter 1]"
    demographic = "age"
    demographic_column = "Age Group-alias"
    data_tableau_table = "Demographics by Age"
    demographic_data = True

    variables = {
        "People with at least one Vaccine Dose": variables.INITIATING_VACCINATIONS_ALL,
        "People with completed Vaccine Series": variables.FULLY_VACCINATED_ALL,
    }

    def fetch(self) -> List[pd.DataFrame]:
        counties = self._retrieve_counties()

        # set filters for each dose type for each county
        results = []
        for county in counties:
            self.filterFunctionValue = county
            for dose in [
                "People with at least one Vaccine Dose",
                "People with completed Vaccine Series",
            ]:
                self.secondaryFilterValue = dose
                tables = self.get_tableau_view()
                results.append(
                    tables[self.data_tableau_table].assign(
                        location_name=county, variable=dose
                    )
                )

        return results

    def normalize(self, data: List[pd.DataFrame]) -> pd.DataFrame:
        df = pd.concat(data)
        df = (
            df.rename(
                columns={
                    "SUM(Vaccination)-alias": "value",
                    self.demographic_column: self.demographic,
                }
            )
            .loc[:, ["location_name", "value", "variable", self.demographic]]
            .pipe(self.extract_CMU, self.variables, skip_columns=[self.demographic])
            .assign(
                dt=self._retrieve_dt("US/Eastern"),
                vintage=self._retrieve_vintage(),
                value=lambda x: pd.to_numeric(
                    x["value"].astype(str).str.replace(",", "")
                ),
            )
            .replace({"75+": "75_plus"})
        )

        if self.demographic == "sex":
            df["sex"] = df["sex"].str.lower()

        return df.drop(columns=["variable"])


class NewYorkVaccineCountySex(NewYorkVaccineCountyAge):
    viewPath = "Gender_Age_Public/VaccinationbyGender"
    demographic = "sex"
    demographic_column = "Demo Value-alias"
    data_tableau_table = "Demographics by Gender"
