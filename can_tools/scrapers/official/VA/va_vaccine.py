import pandas as pd
import us

from can_tools.scrapers.base import CMU
from can_tools.scrapers.official.base import TableauDashboard


class VirginiaStateVaccine(TableauDashboard):
    provider = "state"
    state_fips = int(us.states.lookup("Virginia").fips)
    source = "https://www.vdh.virginia.gov/coronavirus/covid-19-vaccine-summary/"
    has_location = True
    baseurl = "https://vdhpublicdata.vdh.virginia.gov"
    viewPath = "VirginiaCOVID-19Dashboard-VaccineSummary/VirginiaCOVID-19VaccineSummary"

    def fetch(self):
        self.filterFunctionName = None
        return self.get_tableau_view()

    def normalize(self, data) -> pd.DataFrame:
        rows = [
            (
                self._retrieve_dt(),
                data["Vaccine One Dose (3)"]["SUM(Vaccine Count)-alias"].iloc[0],
                data["Vaccine Fully Vacinated (2)"][
                    "SUM(Fully Vaccinated (1))-alias"
                ].iloc[0],
            )
        ]
        df = pd.DataFrame.from_records(
            rows, columns=["dt", "totalFirstDoses", "totalSecondDoses"]
        )
        crename = {
            "totalFirstDoses": CMU(
                category="total_vaccine_initiated",
                measurement="cumulative",
                unit="people",
            ),
            "totalSecondDoses": CMU(
                category="total_vaccine_completed",
                measurement="cumulative",
                unit="people",
            ),
        }
        df = (
            df.query("dt >= '2020-01-01'")
            .melt(id_vars=["dt"], value_vars=crename.keys())
            .dropna()
        )
        df = self.extract_CMU(df, crename)

        df["location"] = self.state_fips
        df["location_name"] = "Virginia"
        df.loc[:, "value"] = pd.to_numeric(df["value"])
        df["vintage"] = self._retrieve_vintage()
        return df.drop(["variable"], axis="columns")


class VirginiaCountyVaccine(TableauDashboard):
    provider = "county"
    state_fips = int(us.states.lookup("Virginia").fips)
    source = "https://www.vdh.virginia.gov/coronavirus/covid-19-vaccine-summary/"
    has_location = False
