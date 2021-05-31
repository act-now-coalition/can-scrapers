import pandas as pd
import us
import os
import multiprocessing as mp

from can_tools.scrapers import variables
from can_tools.scrapers.official.base import TableauDashboard

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

    data_tableau_table = "County Map"
    location_name_col = "County -alias"
    timezone = "US/Eastern"
    filterFunctionName = "[Parameters].[Param.Program (copy)_419679211994820608]"  # fetch from all providers

    # map wide form column names into CMUs
    cmus = {
        "AGG(Calc.Tooltip At Least One Dose Vaccinated)-alias": variables.INITIATING_VACCINATIONS_ALL,
        "AGG(Calc.Tooltip Fully Vaccinated)-alias": variables.FULLY_VACCINATED_ALL,
    }

    def fetch(self):
        self.filterFunctionValue = "3"
        return self.get_tableau_view()[self.data_tableau_table]

    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        df = super().normalize(df)
        df.location_name = df.location_name.str.replace(
            " County", ""
        ).str.strip()  # Strip whitespace
        df.loc[df["location_name"] == "Mcdowell", "location_name"] = "McDowell"
        return df


class NCVaccineAge(NCVaccine):
    viewPath = "NCDHHS_COVID-19_Dashboard_Vaccinations/Demographics"
    timezone = "US/Eastern"
    filterFunctionName = "[Parameters].[Parameter 3 1]"  # county
    secondaryFilterFunctionName = (
        "[Parameters].[Param.DemographicMetric (copy)_1353894643018190853]"  # dose type
    )
    thirdFilterFunctionName = "[Parameters].[Parameter 2]"  # specify providers
    thirdFilterFunctionValue = "3"

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
        path = os.path.dirname(__file__) + "/../../../bootstrap_data/locations.csv"
        counties = list(
            pd.read_csv(path).query("state == @self.state_fips and location != @self.state_fips")["name"]
        )
        
        numprocs = 10 # set s.t 100 % numprocs = 0
        return_list = mp.Manager().list() # global list each process reports back to
        procs = []
        curr = 0 # current location in array (of counties)
        by = int(len(counties)/numprocs) # number of counties per process
        
        for i in range(0, numprocs):
            print('starting proc ', i, ' for indices: [', curr, ',', curr+by-1, ']')
            proc = mp.Process(target=self._get_data, args=(counties[curr : curr + by], return_list))
            curr += by
            procs.append(proc)
            proc.start()
        
        for proc in procs:
            proc.join()
        
        return pd.concat(return_list)
        
    def _get_data(self, counties, data):
        for county in counties:
            self.filterFunctionValue = county + " County"
            for dose_val in ["3", "4"]:
                print('working on:', county, 'dose: ', dose_val)
                self.secondaryFilterValue = dose_val
                df = self.get_tableau_view()[self.worksheet]
                data.append(df.assign(location_name=county))
                

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
