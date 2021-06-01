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
    fetch_url = "https://gis.ne.gov/Enterprise/rest/services/C19Combine/FeatureServer/{table}/query"
    source_name = "Nebraska Department of Health and Human Services"

    crename = {
        "Fully Vac": variables.FULLY_VACCINATED_ALL,
        "All": variables.TOTAL_DOSES_ADMINISTERED_ALL,
    }

    demographic = "sex"
    # table to query to find corresponding data
    table_id = 1
    # dict to change demographic variables to match CMU formatting
    key_replace = {
        "attributes.MALE": "male",
        "attributes.FEMALE": "female",
        "attributes.UNK_SEX": "unknown",
    }
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

    def _filter(self, df):
        return df[
            df["index"].str.contains("MALE") | df["index"].str.contains("UNK_SEX")
        ]

    def normalize(self, data) -> pd.DataFrame:
        df = pd.json_normalize(data["features"])
        df = df.transpose()
        # set column names, filter columns, and remove junk row
        df.columns = df.iloc[1]
        df = df.reset_index()
        df.columns.name = None
        df = df[2:][["index", "Fully Vac", "All"]]

        df = self._filter(df)
        df[self.demographic] = df["index"]
        df = df.assign(
            location=self.state_fips,
            dt=self._retrieve_dt("US/Central"),
        ).drop(columns={"index"})
        out = self._reshape_variables(
            data=df,
            variable_map=self.crename,
            id_vars=[self.demographic],
            skip_columns=[self.demographic],
        )
        return out.replace(self.key_replace)


class NebraskaVaccineRace(NebraskaVaccineSex):
    demographic = "race"
    key_replace = {
        "race": {
            "nativeamerican": "ai_an",
            "africanamerican": "black",
            "pacific": "pacific_islander",
            "multi": "multiple",
        }
    }

    def _filter(self, df):
        df = df[df["index"].str.contains("attributes.race_")]
        df["index"] = df["index"].str.replace("attributes.race_", "").str.lower()
        return df


class NebraskaVaccineEthnicity(NebraskaVaccineSex):
    demographic = "ethnicity"
    key_replace = {
        "ethnicity": {
            "nonhispanic": "non-hispanic",
        }
    }

    def _filter(self, df):
        df = df[df["index"].str.contains("attributes.ethnic_")]
        df["index"] = df["index"].str.replace("attributes.ethnic_", "").str.lower()
        return df


class NebraskaVaccineAge(NebraskaVaccineSex):
    demographic = "age"
    key_replace = {"age": {"85UP": "85_plus"}}

    def _filter(self, df):
        df = df[df["index"].str.contains("attributes.Age_")]
        df["index"] = (
            df["index"].str.replace("attributes.Age_", "").str.replace("_", "-")
        )
        return df
