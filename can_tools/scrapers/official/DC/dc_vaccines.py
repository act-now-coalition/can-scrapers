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
    data_tableau_table = "TimeTable"

    variables = {
        "FULLY VACCINATED": variables.FULLY_VACCINATED_ALL,
        "PARTIALLY/FULLY VACCINATED": variables.INITIATING_VACCINATIONS_ALL,
    }

    def _get_date(self):
        # 'last updated' date is stored in a 1x1 df
        df = self.get_tableau_view(
            url=(self.baseurl + "/views/Vaccine_Public/Administration")
        )["Admin Update"]
        return pd.to_datetime(df.iloc[0]["MaxDate-alias"]).date()

    def normalize(self, data):
        df = data
        df["Measure Values-alias"] = pd.to_numeric(
            df["Measure Values-alias"].str.replace(",", ""), errors="coerce"
        )
        df = df.loc[df["Resident_Type-value"] == "DC Resident"][
            ["Measure Values-alias", "Measure Names-alias"]
        ]
        df["location"] = self.state_fips
        df = (
            df.pivot(
                index="location",
                columns="Measure Names-alias",
                values="Measure Values-alias",
            )
            .reset_index()
            .rename_axis(None, axis=1)
        )
        df["dt"] = self._get_date()

        out = self._reshape_variables(df, self.variables)
        return out


class DCVaccineDemographics(DCVaccine):
    fullUrl = "https://dataviz1.dc.gov/t/OCTO/views/Vaccine_Public/Demographics"
    variables = {
        "FULLY VACCINATED": variables.FULLY_VACCINATED_ALL,
        "INITIATED": variables.INITIATING_VACCINATIONS_ALL,
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
                workbook = workbook.setParameter("Demographic", p)
            df = workbook.getWorksheet("Demographics ").data
            dfs[p.replace(" ", "")] = df
        return dfs

    def normalize(self, data: dict):
        dfs = []
        # loop thru each df and manipulate
        for name, df in data.items():
            # name is the demographic of the df
            # df is the data
            df = df.rename(
                columns={
                    "Vaccination Status-alias": "variable",
                    "SUM(Vaccinated)-value": "value",
                    "Cross-value": name,
                }
            ).drop(
                columns={
                    "SUM(Vaccinated)-alias",
                    "Cross-alias",
                }
            )

            # there are 2 'other' categories, only one has actual data (captial OTHER)
            # i've removed the second one, as it breaks the summation
            df = df[df[name] != "Other"]
            df[name] = df[name].str.lower()

            # sum partially and fully vaccinated people to find at least one dose
            q = 'variable == "{v} VACCINATED" and {demo} == "{d}"'
            for d in df[name].unique():
                initiated_value = int(
                    df.query(q.format(v="PARTIALLY", d=d, demo=name))["value"]
                ) + int(df.query(q.format(v="FULLY", d=d, demo=name))["value"])
                row = {"variable": "INITIATED", name: d, "value": initiated_value}
                df = df.append(row, ignore_index=True)
            dfs.append(df)

        # combine dfs
        out = pd.concat(dfs)
        # remove strictly one-dose column
        out = out.query('variable != "PARTIALLY VACCINATED"')
        out.fillna("all", inplace=True)

        name_to_cmu = {
            "Gender": "sex",
            "AgeGroup": "age",
            "Race": "race",
            "Ethnicity": "ethnicity",
        }
        out = (
            out.pipe(self.extract_CMU, cmu=self.variables)
            .assign(
                value=out["value"].astype(int),
                vintage=self._retrieve_vintage(),
                dt=self._get_date(),
                location=self.state_fips,
            )
            .drop(columns={"age", "race", "ethnicity", "sex", "variable"})
            .rename(columns=name_to_cmu)
            .replace({"65+": "65_plus", "not hispanic": "non-hispanic"})
        )

        return out
