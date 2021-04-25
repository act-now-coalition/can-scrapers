import pandas as pd
import us
import numpy as np
from can_tools.scrapers.base import CMU
from can_tools.scrapers.official.base import ArcGIS


class LAVaccineCounty(ArcGIS):
    provider = "state"
    ARCGIS_ID = "O5K6bb5dZVZcTo5M"
    has_location = True
    location_type = "county"
    state_fips = int(us.states.lookup("Louisiana").fips)
    source = "https://ladhh.maps.arcgis.com/apps/opsdashboard/index.html#/7b2a0703a129451eaaf9046c213331ed"
    source_name = "Louisiana Department of Health"

    def fetch(self):
        return self.get_all_jsons("Vaccinations_by_Race_by_Parish", 0, 5)

    def normalize(self, data):
        df = self.arcgis_jsons_to_df(data)
        df = df.rename(
            columns={
                "PFIPS": "location",
            }
        )
        df["dt"] = self._retrieve_dt("US/Central")

        melted = df.melt(
            id_vars=["dt", "location"], value_vars=["SeriesInt", "SeriesComp"]
        )

        crename = {
            "SeriesInt": CMU(
                category="total_vaccine_initiated",
                measurement="cumulative",
                unit="people",
            ),
            "SeriesComp": CMU(
                category="total_vaccine_completed",
                measurement="cumulative",
                unit="people",
            ),
        }

        result = self.extract_CMU(melted, crename)

        result["vintage"] = self._retrieve_vintage()

        return result.drop(["variable"], axis="columns")


class LAVaccineCountyDemographics(LAVaccineCounty):
    variables = {
        "PercInt_Black_value": CMU(
            category="total_vaccine_initiated",
            measurement="cumulative",
            unit="people",
            race="black",
        ),
        "PercInt_White_value": CMU(
            category="total_vaccine_initiated",
            measurement="cumulative",
            unit="people",
            race="white",
        ),
        "PercInt_Other_value": CMU(
            category="total_vaccine_initiated",
            measurement="cumulative",
            unit="people",
            race="other",
        ),
        "PercInt_RaceUnk_value": CMU(
            category="total_vaccine_initiated",
            measurement="cumulative",
            unit="people",
            race="unknown",
        ),
        "PercComp_Black_value": CMU(
            category="total_vaccine_completed",
            measurement="cumulative",
            unit="people",
            race="black",
        ),
        "PercComp_Other_value": CMU(
            category="total_vaccine_completed",
            measurement="cumulative",
            unit="people",
            race="other",
        ),
        "PercComp_RaceUnk_value": CMU(
            category="total_vaccine_completed",
            measurement="cumulative",
            unit="people",
            race="unknown",
        ),
        "PercComp_White_value": CMU(
            category="total_vaccine_completed",
            measurement="cumulative",
            unit="people",
            race="white",
        ),
        "PercInt_5to17_value": CMU(
            category="total_vaccine_initiated",
            measurement="cumulative",
            unit="people",
            age="5-17",
        ),
        "PercInt_18to29_value": CMU(
            category="total_vaccine_initiated",
            measurement="cumulative",
            unit="people",
            age="18-29",
        ),
        "PercInt_30to39_value": CMU(
            category="total_vaccine_initiated",
            measurement="cumulative",
            unit="people",
            age="30-39",
        ),
        "PercInt_40to49_value": CMU(
            category="total_vaccine_initiated",
            measurement="cumulative",
            unit="people",
            age="40-49",
        ),
        "PercInt_50to59_value": CMU(
            category="total_vaccine_initiated",
            measurement="cumulative",
            unit="people",
            age="50-59",
        ),
        "PercInt_60to69_value": CMU(
            category="total_vaccine_initiated",
            measurement="cumulative",
            unit="people",
            age="60-69",
        ),
        "PercInt_70plus_value": CMU(
            category="total_vaccine_initiated",
            measurement="cumulative",
            unit="people",
            age="70_plus",
        ),
        "PercInt_AgeUnk_value": CMU(
            category="total_vaccine_initiated",
            measurement="cumulative",
            unit="people",
            age="unknown",
        ),
        "PercComp_5to17_value": CMU(
            category="total_vaccine_completed",
            measurement="cumulative",
            unit="people",
            age="5-17",
        ),
        "PercComp_18to29_value": CMU(
            category="total_vaccine_completed",
            measurement="cumulative",
            unit="people",
            age="18-29",
        ),
        "PercComp_30to39_value": CMU(
            category="total_vaccine_completed",
            measurement="cumulative",
            unit="people",
            age="30-39",
        ),
        "PercComp_40to49_value": CMU(
            category="total_vaccine_completed",
            measurement="cumulative",
            unit="people",
            age="40-49",
        ),
        "PercComp_50to59_value": CMU(
            category="total_vaccine_completed",
            measurement="cumulative",
            unit="people",
            age="50-59",
        ),
        "PercComp_60to69_value": CMU(
            category="total_vaccine_completed",
            measurement="cumulative",
            unit="people",
            age="60-69",
        ),
        "PercComp_70plus_value": CMU(
            category="total_vaccine_completed",
            measurement="cumulative",
            unit="people",
            age="70_plus",
        ),
        "PercInt_Female_value": CMU(
            category="total_vaccine_initiated",
            measurement="cumulative",
            unit="people",
            sex="female",
        ),
        "PercInt_Male_value": CMU(
            category="total_vaccine_initiated",
            measurement="cumulative",
            unit="people",
            sex="male",
        ),
        "PercInt_SexUnk_value": CMU(
            category="total_vaccine_initiated",
            measurement="cumulative",
            unit="people",
            sex="unknown",
        ),
        "PercComp_Female_value": CMU(
            category="total_vaccine_completed",
            measurement="cumulative",
            unit="people",
            sex="female",
        ),
        "PercComp_Male_value": CMU(
            category="total_vaccine_completed",
            measurement="cumulative",
            unit="people",
            sex="male",
        ),
        "PercComp_SexUnk_value": CMU(
            category="total_vaccine_completed",
            measurement="cumulative",
            unit="people",
            sex="unknown",
        ),
    }

    def normalize(self, data):
        df = self.arcgis_jsons_to_df(data)
        # Multiply each of these columns by the population column
        init_col_names = [x.replace("_value", "") for x in self.variables.keys()]

        for col in init_col_names:
            df[col + "_value"] = np.floor((df[col] / 100) * df["Total_2018pop"])

        df = self._rename_or_add_date_and_location(
            df, location_column="PFIPS", timezone="US/Eastern"
        )
        df = self._reshape_variables(df, self.variables)

        return df
