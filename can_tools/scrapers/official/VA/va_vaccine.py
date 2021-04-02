import pandas as pd
import us

from can_tools.scrapers.base import ScraperVariable
from can_tools.scrapers.official.base import TableauDashboard


class VirginiaVaccine(TableauDashboard):
    state_fips = int(us.states.lookup("Virginia").fips)
    source = "https://www.vdh.virginia.gov/coronavirus/covid-19-vaccine-summary/"
    source_name = "Virginia Department of Health"
    baseurl = "https://vdhpublicdata.vdh.virginia.gov"
    provider = "state"
    has_location = True
    location_type = ""

    def fetch(self):
        self.filterFunctionName = None
        self.viewPath = (
            "VirginiaCOVID-19Dashboard-VaccineSummary/VirginiaCOVID-19VaccineSummary"
        )

        return self.get_tableau_view()

    def normalize(self, data) -> pd.DataFrame:
        rows = [
            (
                data["Vaccine One Dose"]["SUM(At Least One Dose)-alias"].iloc[0],
                data["Vaccine Fully Vacinated"]["SUM(Fully Vaccinated)-alias"].iloc[0],
                data["Vaccine Total Doses"]["SUM(Vaccine Count)-alias"].iloc[0],
                self.state_fips,
                "Virginia",
            )
        ]
        state_df = pd.DataFrame.from_records(
            rows,
            columns=[
                "totalHadFirstDose",
                "totalHadSecondDose",
                "totalDoses",
                "location",
                "location_name",
            ],
        )
        county_df = data["Doses Administered by Administration FIPS"].rename(
            columns={
                "SUM(Fully Vaccinated)-alias": "totalHadSecondDose",
                "SUM(At Least One Dose)-alias": "totalHadFirstDose",
                "SUM(Vaccine Count)-alias": "totalDoses",
                "Recipient FIPS - Manassas Fix-alias": "location",
                "ATTR(Locality Name)-alias": "location_name",
            }
        )

        df = pd.concat([state_df, county_df], axis=0)

        crename = {
            "totalHadFirstDose": ScraperVariable(
                category="total_vaccine_initiated",
                measurement="cumulative",
                unit="people",
            ),
            "totalHadSecondDose": ScraperVariable(
                category="total_vaccine_completed",
                measurement="cumulative",
                unit="people",
            ),
            "totalDoses": ScraperVariable(
                category="total_vaccine_doses_administered",
                measurement="cumulative",
                unit="doses",
            ),
        }
        df = df.melt(id_vars=["location"], value_vars=crename.keys()).dropna()
        df = self.extract_scraper_variables(df, crename)

        df.loc[:, "value"] = pd.to_numeric(df["value"])
        df.loc[:, "location"] = df["location"].astype(int)
        df["location_type"] = "county"
        df.loc[df["location"] == 51, "location_type"] = "state"
        df["dt"] = self._retrieve_dt()
        df["vintage"] = self._retrieve_vintage()
        return df.drop(["variable"], axis="columns")
