import pandas as pd
import us

from can_tools.scrapers import CMU, variables
from can_tools.scrapers.official.base import ArcGIS

pd.options.mode.chained_assignment = None  # Avoid unnessacary SettingWithCopy warning


class ALCountyVaccine(ArcGIS):
    ARCGIS_ID = "4RQmZZ0yaZkGR1zy"
    has_location = True
    location_type = "county"
    state_fips = int(us.states.lookup("Alabama").fips)
    source = "https://alpublichealth.maps.arcgis.com/apps/opsdashboard/index.html#/e4a232feb1344ce0afd9ac162f3ac4ba"
    source_name = "Alabama Department of Public Health"

    variables = {
        "PERSONVAX": variables.INITIATING_VACCINATIONS_ALL,
        "PERSONCVAX": variables.FULLY_VACCINATED_ALL,
        "NADMIN_RES": variables.TOTAL_DOSES_ADMINISTERED_ALL,
    }
    service = "Vaccination_Dashboard_AGOL_v4_PUBLIC_VIEW"
    sheet = 1

    def fetch(self):
        service = "Vax_Dashboard_Public_3_VIEW"
        return self.get_all_jsons(self.service, self.sheet, "7")

    def normalize(self, data):
        data = self.arcgis_jsons_to_df(data)
        data = self._rename_or_add_date_and_location(
            data, location_column="CNTYFIPS", timezone="US/Central"
        )
        data = self._reshape_variables(data, self.variables)
        locations_to_drop = [0, 99999]
        data = data.query("location != @locations_to_drop")
        return data


class ALCountyVaccineSex(ALCountyVaccine):
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

    variable_columns = ["RECIP_SEX"]
    sheet_num = 6
    service = "Vaccination_Dashboard_AGOL_v4_PUBLIC_VIEW"

    def fetch(self):
        service = "Vaccination_Dashboard_AGOL_v4_PUBLIC_VIEW"
        return self.get_all_jsons(service, self.sheet_num, "7")

    def normalize(self, data):
        df = self.arcgis_jsons_to_df(data)
        df = df.pivot_table(
            index="CNTYFIPS", columns=self.variable_columns, values="COUNTS"
        ).reset_index()
        df = df.rename_axis(None, axis=1)
        df = self._rename_or_add_date_and_location(
            df, location_column="CNTYFIPS", timezone="US/Central"
        )
        df = self._reshape_variables(df, self.variables)
        locations_to_drop = [0, 99999]
        df = df.query("location != @locations_to_drop")
        return df


class ALCountyVaccineRace(ALCountyVaccineSex):
    variable_columns = ["RACE_LBL"]
    sheet_num = 4
    variables = {
        "Native Hawaiian or other Pacific Islander": CMU(
            category="total_vaccine_initiated",
            measurement="cumulative",
            unit="people",
            race="pacific_islander",
        ),
        "Two or More Races": CMU(
            category="total_vaccine_initiated",
            measurement="cumulative",
            unit="people",
            race="multiple",
        ),
        "Other Race": CMU(
            category="total_vaccine_initiated",
            measurement="cumulative",
            unit="people",
            race="other",
        ),
        "American Indian or Alaskan Native": CMU(
            category="total_vaccine_initiated",
            measurement="cumulative",
            unit="people",
            race="ai_an",
        ),
        "White": CMU(
            category="total_vaccine_initiated",
            measurement="cumulative",
            unit="people",
            race="white",
        ),
        "Unknown": CMU(
            category="total_vaccine_initiated",
            measurement="cumulative",
            unit="people",
            race="unknown",
        ),
        "Black or African American": CMU(
            category="total_vaccine_initiated",
            measurement="cumulative",
            unit="people",
            race="black",
        ),
        "Asian": CMU(
            category="total_vaccine_initiated",
            measurement="cumulative",
            unit="people",
            race="asian",
        ),
    }


class ALCountyVaccineAge(ALCountyVaccineSex):
    variable_columns = ["AGECAT"]
    sheet_num = 5
    variables = {
        "16-54": CMU(
            category="total_vaccine_initiated",
            measurement="cumulative",
            unit="people",
            age="16-54",
        ),
        "55-64": CMU(
            category="total_vaccine_initiated",
            measurement="cumulative",
            unit="people",
            age="55-64",
        ),
        "65-74": CMU(
            category="total_vaccine_initiated",
            measurement="cumulative",
            unit="people",
            age="65-74",
        ),
        "75+": CMU(
            category="total_vaccine_initiated",
            measurement="cumulative",
            unit="people",
            age="75_plus",
        ),
    }
