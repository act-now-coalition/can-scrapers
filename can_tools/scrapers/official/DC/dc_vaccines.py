import pandas as pd
import us

from can_tools.scrapers import variables
from can_tools.scrapers.official.base import TableauDashboard


class DCVaccineRace(TableauDashboard):
    has_location = True
    source = "https://coronavirus.dc.gov/data/vaccination"
    source_name = "DC Health"
    state_fips = int(us.states.lookup("District of Columbia").fips)
    location_type = "state"
    baseurl = "https://dataviz1.dc.gov/t/OCTO"
    viewPath = "Vaccine_Public/Demographics"

    data_tableau_table = "Demographics "

    # map column names into CMUs
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

        # sum the partially and fully vaccinated entries to match definition
        # to avoid pivoting to wide then back to long I selected each corresponding entry to sum
        q = 'variable == "{v} VACCINATED" and demo_val == "{d}"'
        for d in ["BLACK", "WHITE", "OTHER"]:
            initiated_value = int(
                df.query(q.format(v="PARTIALLY", d=d))["value"]
            ) + int(df.query(q.format(v="FULLY", d=d))["value"])
            row = {"variable": "INITIATED", "demo_val": d, "value": initiated_value}
            df = df.append(row, ignore_index=True)

        df = df.query('variable != "PARTIALLY VACCINATED"').pipe(
            self.extract_CMU, cmu=self.variables
        )

        out = df.assign(
            race=df["demo_val"].str.lower(),
            value=df["value"].astype(int),
            vintage=self._retrieve_vintage(),
            dt=self._get_date(),
            location=self.state_fips,
        ).drop(columns={"demo_val", "variable"})

        return out
