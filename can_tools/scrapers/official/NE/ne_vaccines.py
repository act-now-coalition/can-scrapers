import pandas as pd
import us
import requests

from can_tools.scrapers import variables
from can_tools.scrapers.official.base import StateDashboard


class NebraskaVaccineSex(StateDashboard):

    has_location = True
    location_type = "state"
    state_fips = int(us.states.lookup("Nebraska").fips)
    source = "https://experience.arcgis.com/experience/ece0db09da4d4ca68252c3967aa1e9dd/page/page_1/"
    fetch_url = (
        "https://gis.ne.gov/Enterprise/rest/services/C19Vac/MapServer/{table}/query"
    )
    source_name = "Nebraska Department of Health and Human Services"

    crename = {
        "initiated": variables.INITIATING_VACCINATIONS_ALL,
        "attributes.TotalCompletedVac": variables.FULLY_VACCINATED_ALL,
        "attributes.TotalDoses": variables.TOTAL_DOSES_ADMINISTERED_ALL,
    }

    # col_name: the demographic column name as labeled in the raw data. cmu_name: name of demo as used in the CMU class
    demographic = {"col_name": "attributes.Gender", "cmu_name": "sex"}
    # table to query to find corresponding data
    table_id = 5
    # dict to change demographic variables to match CMU formatting
    key_replace = {"sex": {"unknown or other": "unknown"}}
    # params for the fetch request
    params = {
        "f": "json",
        "where": "0=0",
        "outFields": "*",
        "returnGeometry": "false",
    }

    def fetch(self):
        res = requests.get(
            self.fetch_url.format(table=self.table_id), params=self.params
        )
        if not res.ok:
            raise ValueError("Could not retrieve data")
        else:
            return res.json()

    def normalize(self, data) -> pd.DataFrame:
        df = pd.json_normalize(data["features"])
        df["location"] = self.state_fips

        # initiated:
        # everyone with at least one dose is the sum of all the single doses, people with one dose, and people with second dose
        df["initiated"] = (
            df["attributes.TotalFirstDose"]
            + df["attributes.TotalSecondDose"]
            + df["attributes.TotalSingleDose"]
        )

        # group by location and demographic and transform
        out = (
            df.melt(
                id_vars=["location", self.demographic["col_name"]],
                value_vars=self.crename.keys(),
            )
            .assign(
                dt=self._retrieve_dt("US/Central"), vintage=self._retrieve_vintage()
            )
            .dropna()
        )
        out.loc[:, "value"] = pd.to_numeric(out["value"])

        # Extract category information and add other variable context
        out = self.extract_CMU(out, self.crename)
        out[self.demographic["cmu_name"]] = out[
            self.demographic["col_name"]
        ].str.lower()

        # fix demo variable formatting
        if self.key_replace:
            out = out.replace(self.key_replace)

        cols_to_keep = [
            "vintage",
            "dt",
            "location",
            "category",
            "measurement",
            "unit",
            "age",
            "race",
            "ethnicity",
            "sex",
            "value",
        ]
        return out.loc[:, cols_to_keep]


class NebraskaVaccineRace(NebraskaVaccineSex):
    table_id = 7
    demographic = {"col_name": "attributes.Race", "cmu_name": "race"}
    key_replace = {
        "race": {
            "american indian or alaska native": "ai_an",
            "black or african-american": "black",
            "native hawaiian or other pacific islander": "pacific_islander",
            "other race": "other",
        }
    }


class NebraskaVaccineEthnicity(NebraskaVaccineSex):
    table_id = 6
    demographic = {"col_name": "attributes.Ethnicity", "cmu_name": "ethnicity"}
    key_replace = {
        "ethnicity": {
            "hispanic or latino": "hispanic",
            "not hispanic or latino": "non-hispanic",
        }
    }


class NebraskaVaccineAge(NebraskaVaccineSex):
    table_id = 3
    demographic = {"col_name": "attributes.AgeGroup", "cmu_name": "age"}
    key_replace = {"age": {"85+": "85_plus", "age unknown": "unknown"}}
