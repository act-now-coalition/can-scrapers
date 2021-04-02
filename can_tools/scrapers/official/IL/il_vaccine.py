import pandas as pd
import requests
import us

from can_tools.scrapers.base import ScraperVariable
from can_tools.scrapers.official.base import StateDashboard


class IllinoisVaccineCounty(StateDashboard):
    has_location = False
    source = "https://www.dph.illinois.gov/covid19/vaccinedata"
    source_name = "Illinois Department of Public Health"
    state_fips = int(us.states.lookup("Illinois").fips)
    url = "https://idph.illinois.gov/DPHPublicInformation/api/covidVaccine/getVaccineAdministrationCurrent"
    location_type = "county"

    def fetch(self) -> dict:
        res = requests.get(self.url)
        if not res.ok:
            msg = f"Could not request data from {self.url}"
            raise ValueError(msg)

        return res.json()

    def normalize_all(self, data) -> pd.DataFrame:
        k = "VaccineAdministration"
        if k not in data:
            raise ValueError(f"Expected to find {k} in JSON response")
        new_names = dict(
            CountyName="location_name",
            Report_Date="dt",
        )
        df = (
            pd.DataFrame(data[k])
            .rename(columns=new_names)
            .assign(dt=lambda x: pd.to_datetime(x["dt"]))
            .set_index(["location_name", "dt"])
        )

        # Select certain columns
        cmus = {
            "AdministeredCount": ScraperVariable(
                category="total_vaccine_doses_administered",
                measurement="cumulative",
                unit="doses",
            ),
            "AllocatedDoses": ScraperVariable(
                category="total_vaccine_allocated",
                measurement="cumulative",
                unit="doses",
            ),
            "PersonsFullyVaccinated": ScraperVariable(
                category="total_vaccine_completed",
                measurement="cumulative",
                unit="people",
            ),
        }

        # Reorganize so we can add Chicago to Cook county
        for cmu_key in cmus.keys():
            df.loc[pd.IndexSlice["Cook", :], cmu_key] = (
                df.loc[pd.IndexSlice["Cook", :], cmu_key].values
                + df.loc[pd.IndexSlice["Chicago", :], cmu_key].values
            )

        return (
            df.reset_index()
            .melt(id_vars=["location_name", "dt"], value_vars=cmus.keys())
            .dropna()
            .assign(
                value=lambda x: pd.to_numeric(x.loc[:, "value"]),
                vintage=self._retrieve_vintage(),
            )
            .pipe(self.extract_ScraperVariable, cmu=cmus)
            .drop(["variable"], axis=1)
        )

    def normalize(self, data) -> pd.DataFrame:
        df = self.normalize_all(data)

        # drop non-county level obs
        non_county = ["Illinois", "Out Of State", "Unknown", "Chicago"]  # noqa
        return df.query("location_name not in @non_county")


class IllinoisVaccineState(IllinoisVaccineCounty):
    has_location = True
    location_type = "state"

    def normalize(self, data) -> pd.DataFrame:
        df = self.normalize_all(data)

        # Keep only state obs, drop location_name, set location
        return (
            df.query("location_name == 'Illinois'")
            .drop(["location_name"], axis=1)
            .assign(location=self.state_fips)
        )
