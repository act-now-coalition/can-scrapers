import pandas as pd
import us

from can_tools.scrapers.base import CMU
from can_tools.scrapers.official.base import TableauDashboard
from can_tools.scrapers import variables


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
        'FULLY VACCINATED': variables.FULLY_VACCINATED_ALL,
        'PARTIALLY/FULLY VACCINATED': variables.INITIATING_VACCINATIONS_ALL
    }

    def normalize(self, data):
        df = data
        df['Measure Values-alias'] =  pd.to_numeric(df['Measure Values-alias'].str.replace(',',''), errors='coerce')
        df = df.loc[df['Resident_Type-value'] == 'DC Resident'][['Measure Values-alias', 'Measure Names-alias']]
        df['location'] = self.state_fips
        df =  df.pivot(index='location', columns='Measure Names-alias', values='Measure Values-alias').reset_index().rename_axis(None, axis=1)

        renamed = self._rename_or_add_date_and_location(df, location_column='location', timezone="US/Eastern")
        out = self._reshape_variables(renamed, self.variables) 
        return out     

