import pandas as pd
import us

from can_tools.scrapers.base import CMU
from can_tools.scrapers.official.base import TableauDashboard
from tableauscraper import TableauScraper as TS


class DCVaccineSex(TableauDashboard):
    has_location = True
    source = "https://coronavirus.dc.gov/data/vaccination"
    source_name = "DC Health"
    state_fips = int(us.states.lookup("District of Columbia").fips)
    location_type = "state"
    baseurl = "https://dataviz1.dc.gov/t/OCTO"
    viewPath = "Vaccine_Public/Demographics"
    demographic_cmu = "sex"
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

        df[self.demographic_cmu] = df["Cross-value"].str.lower()
        out = df.drop(columns={"Cross-value", "variable"})

        return out


class DCVaccineRace(DCVaccineSex):
    fullUrl = "https://dataviz1.dc.gov/t/OCTO/views/Vaccine_Public/Demographics"
    demographic_cmu = "race"
    demographic_col_name = "Race"

    def fetch(self):
        """
        uses the tableauscraper module:
        https://github.com/bertrandmartel/tableau-scraping/blob/master/README.md
        """
        ts = TS()
        ts.loads(self.fullUrl)
        workbook = ts.getWorkbook()
        workbook = workbook.setParameter("Demographic", self.demographic_col_name)
        return workbook.worksheets[0].data


class DCVaccineEthnicity(DCVaccineRace):
    demographic_cmu = "ethinicity"
    demographic_col_name = "Ethnicity"


class DCVaccineAge(DCVaccineRace):
    demographic_cmu = "age"
    demographic_col_name = "Age Group"

    def normalize(self, data):
        df = super().normalize(data)
        return df.replace({"age": {"65+": "65_plus"}})
