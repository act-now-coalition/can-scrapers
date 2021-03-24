import pandas as pd
import us

from can_tools.scrapers.base import CMU
from can_tools.scrapers.official.base import TableauDashboard


class DCVaccineSex(TableauDashboard):
    has_location = True
    source = "https://coronavirus.dc.gov/data/vaccination"
    source_name = "DC Health"
    state_fips = int(us.states.lookup("District of Columbia").fips)
    location_type = "state"
    baseurl = "https://dataviz1.dc.gov/t/OCTO"
    viewPath = "Vaccine_Public/Demographics"

    data_tableau_table = "Demographics "

    # map column names into CMUs
    cmus = {
        "PARTIALLY VACCINATED": CMU(
            category="total_vaccine_initiated", measurement="cumulative", unit="people"
        ),
        "FULLY VACCINATED": CMU(
            category="total_vaccine_completed", measurement="cumulative", unit="people"
        ),
    }

    def _get_date(self):
        # 'last updated' date is stored in a 1x1 df
        df = self.get_tableau_view(
            url=(self.baseurl + "/views/Vaccine_Public/Administration")
        )["Update"]
        return pd.to_datetime(df.iloc[0]["MaxDate-alias"]).date()

    def normalize(self, data):
        df = (
            data.rename(
                columns={
                    "Vaccination Status-alias": "variable",
                    "SUM(Vaccinated)-value": "value",
                }
            )
            .assign(
                vintage=self._retrieve_vintage(),
                dt=self._get_date(),
                location=self.state_fips,
            )
            .drop(
                columns={
                    "SUM(Vaccinated)-alias",
                    "Cross-alias",
                }
            )
        )

        # already in long form yay
        df = df.pipe(self.extract_CMU, cmu=self.cmus)
        df["value"] = df["value"].astype(int)

        out = df.assign(sex=df["Cross-value"].str.lower()).drop(
            columns={"Cross-value", "variable"}
        )

        return out
