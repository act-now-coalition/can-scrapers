import pandas as pd
import us

from can_tools.scrapers.base import CMU
from can_tools.scrapers.official.base import StateDashboard


class MichiganVaccineCounty(StateDashboard):
    has_location = False
    source = "https://www.michigan.gov/coronavirus/0,9753,7-406-98178_103214_103272-547150--,00.html"
    state_fips = int(us.states.lookup("Michigan").fips)
    url = "https://www.michigan.gov/documents/flu/Covid_Vaccine_Doses_Administered_710815_7.xlsx"
    location_type = "county"

    def fetch(self):
        return pd.read_excel(self.url, sheet_name="Doses Administered")

    def normalize(self, data: pd.DataFrame) -> pd.DataFrame:
        # date is written out in first column name
        data["variable"] = data["Vaccine Type"] + data["Dose Number"]

        colnames = {
            "Person's Residence in County": "location_name",
            "Data as of": "dt",
            "Number of Doses": "value",
        }
        cmus = {
            "ModernaFirst Dose": CMU(
                category="moderna_vaccine_initiated",
                measurement="cumulative",
                unit="people",
            ),
            "ModernaSecond Dose": CMU(
                category="moderna_vaccine_completed",
                measurement="cumulative",
                unit="people",
            ),
            "PfizerFirst Dose": CMU(
                category="pfizer_vaccine_initiated",
                measurement="cumulative",
                unit="people",
            ),
            "PfizerSecond Dose": CMU(
                category="pfizer_vaccine_completed",
                measurement="cumulative",
                unit="people",
            ),
        }
        not_counties = ["No County", "Detroit"]  # noqa
        return (
            data
            .rename(columns=colnames)
            .loc[:, ["location_name", "dt", "variable", "value"]]
            .query("location_name not in  @not_counties")
            .assign(
                value=lambda x: pd.to_numeric(x.loc[:, "value"]),
                vintage=self._retrieve_vintage(),
            )
            .pipe(self.extract_CMU, cmu=cmus)
            .drop(["variable"], axis=1)
        )
