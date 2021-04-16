import pandas as pd
from us import states
import sqlalchemy as sa
from can_tools.scrapers.official.base import StateDashboard
from can_tools.scrapers import variables, CMU


class PhiladelphaVaccine(StateDashboard):
    state_fips = int(states.lookup("Pennsylvania").fips)
    has_location = True
    location_type = "county"
    source = "https://www.opendataphilly.org/dataset/covid-vaccinations"
    source_name = "OpenDataPhilly"
    url = "https://phl.carto.com/api/v2/sql?filename=covid_vaccine_totals&format=csv&skipfields=cartodb_id,the_geom,the_geom_webmercator&q=SELECT%20*%20FROM%20covid_vaccine_totals"
    county_fips = 42101
    age_url = "https://phl.carto.com/api/v2/sql?filename=covid_vaccines_by_age&format=csv&skipfields=cartodb_id,the_geom,the_geom_webmercator&q=SELECT%20*%20FROM%20covid_vaccines_by_age"
    sex_url = "https://phl.carto.com/api/v2/sql?filename=covid_vaccines_by_sex&format=csv&skipfields=cartodb_id,the_geom,the_geom_webmercator&q=SELECT%20*%20FROM%20covid_vaccines_by_sex"
    race_url = "https://phl.carto.com/api/v2/sql?filename=covid_vaccines_by_race&format=csv&skipfields=cartodb_id,the_geom,the_geom_webmercator&q=SELECT%20*%20FROM%20covid_vaccines_by_race"
    variables = {
        "partially_vaccinated": variables.INITIATING_VACCINATIONS_ALL,
        "fully_vaccinated": variables.FULLY_VACCINATED_ALL,
    }

    def fetch(self):

        return {
            "age": pd.read_csv(self.age_url, parse_dates=True),
            "sex": pd.read_csv(self.sex_url, parse_dates=True),
            "race": pd.read_csv(self.race_url, parse_dates=True),
            "all": pd.read_csv(self.url, parse_dates=True),
        }

    def _normalize_general(self, df):
        df["location_id"] = self.county_fips
        df = self._rename_or_add_date_and_location(
            df, date_column="etl_timestamp", location_column="location_id"
        )

        out = self._reshape_variables(df, self.variables)

        return out

    def _normalize_sex(self, df):
        df.sex = df.sex.replace("M", "male")
        df.sex = df.sex.replace("F", "female")
        df.sex = df.sex.replace("Other/Missing", "unknown")
        df["location_id"] = self.county_fips
        df = self._rename_or_add_date_and_location(
            df, date_column="etl_timestamp", location_column="location_id"
        )

        df = df.melt(id_vars=["location", "dt", "sex"])

        out = self.extract_CMU(
            df,
            self.variables,
            [
                "category",
                "measurement",
                "unit",
                "age",
                "race",
                "ethnicity",
            ],
        )
        return out

    def _normalize_age(self, df):
        df.age = df.age.str.replace("+", "_plus")
        df.age = df.age.str.replace("<", "0-")

        df["location_id"] = self.county_fips
        df = self._rename_or_add_date_and_location(
            df, date_column="etl_timestamp", location_column="location_id"
        )

        df = df.melt(id_vars=["location", "dt", "age"])
        out = self.extract_CMU(
            df,
            self.variables,
            [
                "category",
                "measurement",
                "unit",
                "sex",
                "race",
                "ethnicity",
            ],
        )
        return out

    def _normalize_race(self, df):
        df = df.rename(columns={"racial_identity": "race"})
        df.race = df.race.replace(
            {
                "African American": "black",
                "Asian": "asian",
                "Other": "other",
                "White": "white",
                "Unknown": "unknown",
                "Hispanic": "hispanic",
            }
        )

        df["location_id"] = self.county_fips
        df = self._rename_or_add_date_and_location(
            df, date_column="etl_timestamp", location_column="location_id"
        )
        df = df.melt(id_vars=["location", "dt", "race"])
        out = self.extract_CMU(
            df,
            self.variables,
            [
                "category",
                "measurement",
                "unit",
                "sex",
                "age",
                "ethnicity",
            ],
        )

        # # fix rows where race == 'hispanic'
        out.loc[df.race == "hispanic", "ethnicity"] = "hispanic"
        out.loc[df.race == "hispanic", "race"] = "all"

        return out

    def normalize(self, data):
        df = pd.concat(
            [
                self._normalize_age(data["age"]),
                self._normalize_general(data["all"]),
                self._normalize_race(data["race"]),
                self._normalize_sex(data["sex"]),
            ]
        ).reset_index(drop=True)

        df.vintage = self._retrieve_vintage()
        return df
