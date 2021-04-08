import pandas as pd
import us

from can_tools.scrapers.base import CMU
from can_tools.scrapers.official.base import ArcGIS


class GeorgiaCountyVaccine(ArcGIS):
    """
    Fetch county level covid data from an ARCGIS dashboard

    Example implementations:
      * `can_tools/scrapers/official/FL/fl_state.py`
      * `can_tools/scrapers/official/GA/ga_vaccines.py`
    """

    ARCGIS_ID = "t7DA9BjRElflTVpw"
    has_location = False
    location_type = "county"
    state_fips = int(us.states.lookup("Georgia").fips)
    source = (
        "https://experience.arcgis.com/experience/3d8eea39f5c1443db1743a4cb8948a9c/"
    )
    source_name = "Georgia Department of Public Health"

    def fetch(self):
        return self.get_all_jsons(
            "Georgia_DPH_COVID19_Vaccination_public_v2_VIEW", 6, 6
        )

    def normalize(self, data):
        df = self.arcgis_jsons_to_df(data)
        df.columns = [c.lower() for c in df.columns]
        # Fix location names and drop state data
        df.loc[:, "location_name"] = (
            df["county_name"].str.title().str.replace("County", "").str.strip()
        )
        df = df.replace(
            {
                "location_name": {
                    "Mcduffie": "McDuffie",
                    "Dekalb": "DeKalb",
                    "Mcintosh": "McIntosh",
                }
            }
        )
        df = df.query("location_name != 'Georgia'")

        crename = {
            "dose_1": CMU(
                category="total_vaccine_initiated",
                measurement="cumulative",
                unit="people",
            ),
            "dose_2": CMU(
                category="total_vaccine_completed",
                measurement="cumulative",
                unit="people",
            ),
            "total_administered": CMU(
                category="total_vaccine_doses_administered",
                measurement="cumulative",
                unit="doses",
            ),
        }

        # Extract category information and add other variable context
        out = df.melt(id_vars=["location_name"], value_vars=crename.keys())
        out = self.extract_CMU(out, crename)

        # Add date and vintage
        out["dt"] = self._retrieve_dt("US/Eastern")
        out["vintage"] = self._retrieve_vintage()
        cols_to_keep = [
            "vintage",
            "dt",
            "location_name",
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


class GeorgiaCountyVaccineAge(GeorgiaCountyVaccine):
    service = "Georgia_DPH_PUBLIC_Vaccination_Dashboard_V5_VIEW"
    has_location = True
    sheet = 7
    column_names = ["AGE"]

    variables = {
        "00-05": CMU(
            category="total_vaccine_initiated",
            measurement="cumulative",
            unit="doses",
            age="0-9",
        ),
        "05_09": CMU(
            category="total_vaccine_initiated",
            measurement="cumulative",
            unit="doses",
            age="5-9",
        ),
        "10_14": CMU(
            category="total_vaccine_initiated",
            measurement="cumulative",
            unit="doses",
            age="10-14",
        ),
        "15_19": CMU(
            category="total_vaccine_initiated",
            measurement="cumulative",
            unit="doses",
            age="15-19",
        ),
        "20_24": CMU(
            category="total_vaccine_initiated",
            measurement="cumulative",
            unit="doses",
            age="20-24",
        ),
        "25_34": CMU(
            category="total_vaccine_initiated",
            measurement="cumulative",
            unit="doses",
            age="25-34",
        ),
        "35_44": CMU(
            category="total_vaccine_initiated",
            measurement="cumulative",
            unit="doses",
            age="35-44",
        ),
        "45_54": CMU(
            category="total_vaccine_initiated",
            measurement="cumulative",
            unit="doses",
            age="45-54",
        ),
        "55_64": CMU(
            category="total_vaccine_initiated",
            measurement="cumulative",
            unit="doses",
            age="55-64",
        ),
        "65_74": CMU(
            category="total_vaccine_initiated",
            measurement="cumulative",
            unit="doses",
            age="65-74",
        ),
        "75_84": CMU(
            category="total_vaccine_initiated",
            measurement="cumulative",
            unit="doses",
            age="75-84",
        ),
        "85PLUS": CMU(
            category="total_vaccine_initiated",
            measurement="cumulative",
            unit="doses",
            age="85_plus",
        ),
    }

    def fetch(self):
        return self.get_all_jsons(self.service, self.sheet, "6")

    def normalize(self, data):
        df = self.arcgis_jsons_to_df(data)
        df = (
            df.pivot_table(
                index="COUNTYFIPS", columns=self.column_names, values="COUNTS"
            )
            .reset_index()
            .rename_axis(None, axis=1)
        )
        df = self._rename_or_add_date_and_location(
            df, location_column="COUNTYFIPS", timezone="US/Eastern"
        )
        df = self._reshape_variables(df, self.variables)

        locs_to_drop = ["0"]
        df = df.query("location not in @locs_to_drop")
        return df


class GeorgiaCountyVaccineRace(GeorgiaCountyVaccineAge):
    sheet = 6
    column_names = ["RACE_ID"]

    variables = {
        "2054-5": CMU(
            category="total_vaccine_initiated",
            measurement="cumulative",
            unit="people",
            race="black",
        ),
        "2076-8": CMU(
            category="total_vaccine_initiated",
            measurement="cumulative",
            unit="people",
            race="white",
        ),
        "2106-3": CMU(
            category="total_vaccine_initiated",
            measurement="cumulative",
            unit="people",
            race="white",
        ),
        "1002-5": CMU(
            category="total_vaccine_initiated",
            measurement="cumulative",
            unit="people",
            race="ai_an",
        ),
        "2028-9": CMU(
            category="total_vaccine_initiated",
            measurement="cumulative",
            unit="people",
            race="multiple",
        ),
        "ANHOPI": CMU(
            category="total_vaccine_initiated",
            measurement="cumulative",
            unit="people",
            race="asian",
        ),
        "UNK": CMU(
            category="total_vaccine_initiated",
            measurement="cumulative",
            unit="people",
            race="unknown",
        ),
        "2131-1": CMU(
            category="total_vaccine_initiated",
            measurement="cumulative",
            unit="people",
            race="other",
        ),
    }
