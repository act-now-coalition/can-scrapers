import pandas as pd
import us

from can_tools.scrapers.base import CMU
from can_tools.scrapers import variables
from can_tools.scrapers.official.base import ArcGIS


class GeorgiaCountyVaccine(ArcGIS):
    """
    Fetch county level covid data from an ARCGIS dashboard

    Example implementations:
      * `can_tools/scrapers/official/FL/fl_state.py`
      * `can_tools/scrapers/official/GA/ga_vaccines.py`
    """

    ARCGIS_ID = "t7DA9BjRElflTVpw"
    has_location = True
    location_type = "county"
    state_fips = int(us.states.lookup("Georgia").fips)
    source = (
        "https://experience.arcgis.com/experience/3d8eea39f5c1443db1743a4cb8948a9c/"
    )
    source_name = "Georgia Department of Public Health"
    service = "Georgia_DPH_PUBLIC_Vaccination_Dashboard_V5_VIEW"
    sheet = 4

    variables = {
        "CUMPERSONCVAX": variables.FULLY_VACCINATED_ALL,
        "CUMPERSONVAX": variables.INITIATING_VACCINATIONS_ALL,
        "CUMVAXADMIN": variables.TOTAL_DOSES_ADMINISTERED_ALL,
    }

    def fetch(self):
        completed = self._fetch_completed()
        initiated = self._fetch_initiated()
        total = self._fetch_total()

        return {"total": total, "completed": completed, "initiated": initiated}

    def _fetch_completed(self):
        sheet = 5
        return self.get_all_jsons(self.service, sheet, "6")

    def _fetch_initiated(self):
        sheet = 4
        return self.get_all_jsons(self.service, sheet, "6")

    def _fetch_total(self):
        sheet = 3
        return self.get_all_jsons(self.service, sheet, "6")

    def normalize_helper(self, data, dataset_name, col):
        df = self.arcgis_jsons_to_df(data[dataset_name])
        df = self._rename_or_add_date_and_location(
            df, date_column="ADMIN_DATE", location_column="COUNTY_ID"
        )
        return df.set_index(["location", "dt"])[[col]]

    def normalize(self, data):
        completed = self.normalize_helper(data, "completed", "CUMPERSONCVAX")
        initiated = self.normalize_helper(data, "initiated", "CUMPERSONVAX")
        total = self.normalize_helper(data, "total", "CUMVAXADMIN")
        df = completed.join(initiated).join(total).reset_index()
        out = self._reshape_variables(df, self.variables)
        locs_to_del = ["0", "99999", 0, 99999]

        return out.loc[~out.location.isin(locs_to_del), :]


class GeorgiaCountyVaccineAge(GeorgiaCountyVaccine):
    service = "Georgia_DPH_PUBLIC_Vaccination_Dashboard_V5_VIEW"
    sheet = 7
    column_names = ["AGE"]

    variables = {
        "00-05": CMU(
            category="total_vaccine_initiated",
            measurement="cumulative",
            unit="people",
            age="0-9",
        ),
        "05_09": CMU(
            category="total_vaccine_initiated",
            measurement="cumulative",
            unit="people",
            age="5-9",
        ),
        "10_14": CMU(
            category="total_vaccine_initiated",
            measurement="cumulative",
            unit="people",
            age="10-14",
        ),
        "15_19": CMU(
            category="total_vaccine_initiated",
            measurement="cumulative",
            unit="people",
            age="15-19",
        ),
        "20_24": CMU(
            category="total_vaccine_initiated",
            measurement="cumulative",
            unit="people",
            age="20-24",
        ),
        "25_34": CMU(
            category="total_vaccine_initiated",
            measurement="cumulative",
            unit="people",
            age="25-34",
        ),
        "35_44": CMU(
            category="total_vaccine_initiated",
            measurement="cumulative",
            unit="people",
            age="35-44",
        ),
        "45_54": CMU(
            category="total_vaccine_initiated",
            measurement="cumulative",
            unit="people",
            age="45-54",
        ),
        "55_64": CMU(
            category="total_vaccine_initiated",
            measurement="cumulative",
            unit="people",
            age="55-64",
        ),
        "65_74": CMU(
            category="total_vaccine_initiated",
            measurement="cumulative",
            unit="people",
            age="65-74",
        ),
        "75_84": CMU(
            category="total_vaccine_initiated",
            measurement="cumulative",
            unit="people",
            age="75-84",
        ),
        "85PLUS": CMU(
            category="total_vaccine_initiated",
            measurement="cumulative",
            unit="people",
            age="85_plus",
        ),
    }

    def fetch(self):
        return self.get_all_jsons(self.service, self.sheet, "6")

    def normalize(self, data):
        df = self.arcgis_jsons_to_df(data)
        df["COUNTS"] = pd.to_numeric(df["COUNTS"], errors="coerce")
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

        locs_to_drop = ["0", "00000", 0]
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
        # "2076-8": CMU(
        #     category="total_vaccine_initiated",
        #     measurement="cumulative",
        #     unit="people",
        #     race="white",
        # ),
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

    def fetch(self):
        return self.get_all_jsons(self.service, self.sheet, "6")


class GeorgiaCountyVaccineSex(GeorgiaCountyVaccineRace):
    sheet = 8
    column_names = ["SEX"]
    variables = {
        "F": CMU(
            category="total_vaccine_initiated",
            measurement="cumulative",
            unit="people",
            sex="female",
        ),
        "M": CMU(
            category="total_vaccine_initiated",
            measurement="cumulative",
            unit="people",
            sex="male",
        ),
        "U": CMU(
            category="total_vaccine_initiated",
            measurement="cumulative",
            unit="people",
            sex="unknown",
        ),
    }


class GeorgiaCountyVaccineEthnicity(GeorgiaCountyVaccineAge):
    sheet = 9
    column_names = ["ETHNICITY"]
    variables = {
        "2186-5": CMU(
            category="total_vaccine_initiated",
            measurement="cumulative",
            unit="people",
            ethnicity="non-hispanic",
        ),
        "2135-2": CMU(
            category="total_vaccine_initiated",
            measurement="cumulative",
            unit="people",
            ethnicity="hispanic",
        ),
        "UNK": CMU(
            category="total_vaccine_initiated",
            measurement="cumulative",
            unit="people",
            ethnicity="unknown",
        ),
    }
