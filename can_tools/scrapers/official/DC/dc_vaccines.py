import pandas as pd
import us

from can_tools.scrapers import variables
from can_tools.scrapers.official.base import TableauDashboard

# TODO: 12/12/2022 broken scraper
class DCVaccineDemographics(TableauDashboard):
    has_location = True
    source = "https://coronavirus.dc.gov/data/vaccination"
    source_name = "DC Health"
    state_fips = int(us.states.lookup("District of Columbia").fips)
    location_type = "state"
    baseurl = "https://dataviz1.dc.gov/t/OCTO"
    viewPath = "Vaccine_Public/Demographics"
    filterFunctionName = "[Parameters].[Parameter 9]"
    variables = {
        "Fully Vaccinated ": variables.FULLY_VACCINATED_ALL,
        "Fully/Partially Vaccinated ": variables.INITIATING_VACCINATIONS_ALL,
    }
    demographic_names = {
        "age": "Age Group",
        "race": "Race",
        "sex": "Gender",
        "ethnicity": "Ethnicity",
    }
    demographic_data = True

    def fetch(self):
        dfs = {}
        for demo, filter in self.demographic_names.items():
            self.filterFunctionValue = filter
            dfs[demo] = self.get_tableau_view()["All-Age-Group-Table"]
        return dfs

    def normalize(self, data):
        dfs = []
        for name, df in data.items():
            if name == "age":
                columns = {
                    "Measure Values-alias": "value",
                    "Age Group 4-alias": "age",
                    "Measure Names-alias": "variable",
                }
                df = df.query("`Age Group 4-alias` != '%all%'")
            else:
                columns = {
                    "Measure Values-alias": "value",
                    "Age Group 4-alias": "age",
                    "p.CrossTab.AgeGroup-alias": name,
                    "Measure Names-alias": "variable",
                }
            df = (
                df.rename(columns=columns)
                .loc[:, columns.values()]
                .query(
                    "variable in ['Fully/Partially Vaccinated ', 'Fully Vaccinated ']"
                )
                .assign(
                    dt=self._retrieve_dt(),
                    vintage=self._retrieve_vintage(),
                    location=11,
                    value=lambda x: pd.to_numeric(x["value"].str.replace(",", "")),
                )
                .replace(f"%all%", "all")
                .pipe(self.extract_CMU, cmu=self.variables, skip_columns=[name, "age"])
            )
            df[name] = df[name].str.lower()
            dfs.append(df)
        out = pd.concat(dfs)
        return out.drop(columns={"variable"}).replace(
            {
                "hispanic or latino": "hispanic",
                "not hispanic or latino": "non-hispanic",
                "asian or pacific islander": "asian_or_pacific_islander",
                "85+": "85_plus",
            }
        )
