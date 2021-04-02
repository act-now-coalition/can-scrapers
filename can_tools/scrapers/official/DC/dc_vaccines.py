import pandas as pd
import us

from can_tools.scrapers import variables
from can_tools.scrapers.official.base import TableauDashboard
from can_tools.scrapers import variables


class DCVaccineRace(TableauDashboard):
    has_location = True
    source = "https://coronavirus.dc.gov/data/vaccination"
    source_name = "DC Health"
    state_fips = int(us.states.lookup("District of Columbia").fips)
    location_type = "state"
    baseurl = "https://dataviz1.dc.gov/t/OCTO"
    viewPath = "Vaccine_Public/Demographics"

    data_tableau_table = "Demographics "

    # map column names into ScraperVariables
    variables = {
        "FULLY VACCINATED": variables.FULLY_VACCINATED_ALL,
        "INITIATED": variables.INITIATING_VACCINATIONS_ALL,
    }

    def _get_date(self):
        # 'last updated' date is stored in a 1x1 df
        df = self.get_tableau_view(
            url=(self.baseurl + "/views/Vaccine_Public/Administration")
        )["Admin Update"]
        return pd.to_datetime(df.iloc[0]["MaxDate-alias"]).date()

    def _get_unknown(self):
        # get total unknown initiating value
        initiated = int(
            self.get_tableau_view()["Sheet 11"]["Measure Values-alias"][0].replace(
                ",", ""
            )
        )

        df = self.get_tableau_view()["Demographics (2)"]
        df = (
            df.rename(
                columns={
                    "Vaccination Status-alias": "variable",
                    "SUM(Vaccinated)-alias": "value",
                    "Cross-alias": "demo_val",
                }
            )
            .drop(columns={"Cross-value", "Vaccination Status-value"})
            .query("demo_val == 'UNKNOWN'")
        )

        # the 'completed' is a fraction of the total initiating value
        df["value"] = (df["value"] * initiated).astype(int)

        # create a new row and append
        row = {"variable": "INITIATED", "demo_val": "UNKNOWN", "value": initiated}
        df = df.append(row, ignore_index=True)
        df = df[
            (df["variable"] == "FULLY VACCINATED") | (df["variable"] == "INITIATED")
        ]

        return df.pipe(self.extract_scraper_variables, cmu=self.variables).assign(
            race="unknown", ethnicity="unknown"
        )

    def normalize(self, data):
        df = data.rename(
            columns={
                "Vaccination Status-alias": "variable",
                "SUM(Vaccinated)-value": "value",
                "Cross-value": "demo_val",
            }
        ).drop(
            columns={
                "SUM(Vaccinated)-alias",
                "Cross-alias",
            }
        )
        df["demo_val"] = df["demo_val"].str.lower()

        # check to ensure that the scraper is still returning data by race
        if not all(x in list(df["demo_val"].unique()) for x in ["white", "black"]):
            raise ValueError("scraper is not returning race data")

        # sum the partially and fully vaccinated entries to match definition
        # to avoid pivoting to wide then back to long I selected each corresponding entry to sum
        q = 'variable == "{v} VACCINATED" and demo_val == "{d}"'
        for d in ["black", "white", "other"]:
            initiated_value = int(
                df.query(q.format(v="PARTIALLY", d=d))["value"]
            ) + int(df.query(q.format(v="FULLY", d=d))["value"])
            row = {"variable": "INITIATED", "demo_val": d, "value": initiated_value}
            df = df.append(row, ignore_index=True)

        df = df.query('variable != "PARTIALLY VACCINATED"').pipe(
            self.extract_scraper_variables, cmu=self.variables
        )

        out = df.assign(race=df["demo_val"], value=df["value"].astype(int))
        # add the unknown values
        out = pd.concat([out, self._get_unknown()])

        out = out.assign(
            vintage=self._retrieve_vintage(),
            dt=self._get_date(),
            location=self.state_fips,
        ).drop(columns={"demo_val", "variable"})
        return out


class DCVaccine(DCVaccineRace):
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
