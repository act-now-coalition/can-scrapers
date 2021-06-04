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
        "Fully/Partially Vaccinated": variables.INITIATING_VACCINATIONS_ALL,
        "Total Administered": variables.TOTAL_DOSES_ADMINISTERED_ALL,
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
                "['Fully Vaccinated', 'Fully/Partially Vaccinated', 'Total Administered']"
                "and `Table Names-value` in" 
                "['DC Resident (outside DC)', 'DC Resident (within DC)']"
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
    fullUrl = "https://dataviz1.dc.gov/t/OCTO/views/Vaccine_Public/Demographics"
    variables = {
        "FULLY VACCINATED": variables.FULLY_VACCINATED_ALL,
        "at_least_one": variables.INITIATING_VACCINATIONS_ALL,
    }

    def fetch(self):
        """
        uses the tableauscraper module:
        https://github.com/bertrandmartel/tableau-scraping/blob/master/README.md
        """
        ts = TS()
        ts.loads(self.fullUrl)
        workbook = ts.getWorkbook()
        params = workbook.getParameters()
        params = list(params[0]["values"])
        # return a dictionary of labelled dataframes for each demographic
        dfs = {}
        for p in params:
            # it appears that 'setting' a parameter to it's default/first value causes issues,
            # so we can get the first parameter's data without setting anything, as that data is already present
            if p != params[0]:
                workbook = workbook.setParameter("Charts", p)
            df = workbook.getWorksheet(f"Coverage - {p} (2)").data
            dfs[p.replace(" ", "")] = df
        return dfs

    def _normalize_demo_group(self, df, groupname):
        demo_replacements = {
            "85+": "85_plus",
            "american indian or alaska native": "ai_an",
            "not hispanic": "non-hispanic",
        }
        colname = None
        finding = groupname if groupname != "sex" else "gender"
        for i in list(df):
            if finding in i.lower() and "-alias" in i.lower():
                colname = i
                break
        else:
            raise ValueError(f"Couldn't find demographic column for {groupname}")

        renames = {
            "Vaccination Status-alias": "variable",
            "SUM(Vaccinated)-alias": "value",
            colname: groupname,
        }
        return (
            df.rename(columns=renames)
            .loc[:, list(renames.values())]
            .assign(
                **{
                    groupname: lambda x: x[groupname]
                    .str.lower()
                    .replace(demo_replacements)
                }
            )
            .replace(
                {
                    "variable": {r"%all%": "at_least_one"},
                }
            )
            .pipe(lambda x: x.loc[x["variable"].isin(self.variables.keys()), :])
            .pipe(self.extract_CMU, cmu=self.variables, skip_columns=[groupname])
            .assign(location=self.state_fips, vintage=self._retrieve_vintage())
            .pipe(
                self._rename_or_add_date_and_location,
                timezone="US/Eastern",
                location_column="location",
            )
            .drop(["variable"], axis="columns")
        )

    def normalize(self, data: dict):
        dfs = []
        demo_cols = {
            "AgeGroup": "age",
            "Race": "race",
            "Ethnicity": "ethnicity",
            "Gender": "sex",
        }
        for k, col in demo_cols.items():
            dfs.append(self._normalize_demo_group(data[k], col))
        return pd.concat(dfs, ignore_index=True, axis=0)
