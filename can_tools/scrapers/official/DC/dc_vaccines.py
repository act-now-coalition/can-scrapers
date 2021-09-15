import pandas as pd
import us

from can_tools.scrapers import variables
from can_tools.scrapers.official.base import TableauDashboard
from tableauscraper import TableauScraper as TS


class DCVaccine(TableauDashboard):
    has_location = True
    source = "https://coronavirus.dc.gov/data/vaccination"
    source_name = "DC Health"
    state_fips = int(us.states.lookup("District of Columbia").fips)
    location_type = "state"
    baseurl = "https://dataviz1.dc.gov/t/OCTO"
    viewPath = "Vaccine_Public/Administration"
    data_tableau_table = "Sheet 29"

    variables = {
        "Fully Vaccinated": variables.FULLY_VACCINATED_ALL,
        "At Least One Dose": variables.INITIATING_VACCINATIONS_ALL,
        "Total Administrations": variables.TOTAL_DOSES_ADMINISTERED_ALL,
    }

    def _get_date(self):
        # 'last updated' date is stored in a 1x1 df
        df = self.get_tableau_view(
            url=(self.baseurl + "/views/Vaccine_Public/Administration")
        )["Admin Update"]
        return pd.to_datetime(df.iloc[0]["MaxDate-alias"]).date()

    def normalize(self, data):
        df = (
            # keep only DC residents (in and out of state)
            data.query(
                "`Measure Names-alias` in"
                "['Fully Vaccinated', 'At Least One Dose', 'Total Administrations']"
                "and `Type of Resident-value` in"
                "['DC Resident (outside DC)', 'DC Resident (within DC)', 'DC Resident (Federal Entity)']"
            )
            .assign(
                value=lambda x: pd.to_numeric(
                    x["Measure Values-alias"].str.replace(",", ""), errors="coerce"
                )
            )
            .rename(columns={"Measure Names-alias": "variable"})
        )

        # combine DC resident (within DC) and DC resident (outside DC) into one variable
        out = (
            df.loc[:, ["value", "variable"]]
            .groupby("variable")
            .sum()
            .reset_index()
            .assign(
                vintage=self._retrieve_vintage(),
                dt=self._get_date(),
                location=self.state_fips,
            )
        )

        # transform
        out = self.extract_CMU(df=out, cmu=self.variables)
        return out.drop(columns="variable")


class DCVaccineDemographics(DCVaccine):
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
