import pandas as pd
import us

from can_tools.scrapers import CMU
from can_tools.scrapers.official.WI.common import WisconsinArcGIS


class WisconsinState(WisconsinArcGIS):
    """
    Fetch state-level covid data from Wisconsin's ARCGIS dashboard
    Includes demographic breakdowns
    """

    has_location = True
    location_type = "state"
    state_fips = int(us.states.lookup("Wisconsin").fips)
    source = "https://www.dhs.wisconsin.gov/covid-19/data.htm"

    def fetch(self):
        return self.get_all_jsons("DHS_COVID19/COVID19_WI", 3, "server")

    def normalize(self, data):
        df = self.arcgis_jsons_to_df(data)
        df.columns = [x.lower() for x in list(df)]
        df["location"] = self.state_fips

        crename = {
            # #
            # # totals across all sexes/ages/races/ethnicities
            # #
            "positive": CMU(
                category="pcr_tests_positive",
                measurement="cumulative",
                unit="unique_people",
            ),
            "pos_new": CMU(
                category="pcr_tests_positive",
                measurement="new",
                unit="unique_people",
            ),
            "deaths": CMU(category="deaths", measurement="cumulative", unit="people"),
            "dth_new": CMU(category="deaths", measurement="new", unit="people"),
            "hosp_yes": CMU(
                category="hospital_beds_in_use_covid",
                measurement="cumulative",
                unit="people",
            ),
            "ic_yes": CMU(
                category="icu_beds_in_use_covid",
                measurement="cumulative",
                unit="people",
            ),
            # #
            # # sex breakdowns
            # #
            # # positive tests by sex
            "pos_male": CMU(
                category="pcr_tests_positive",
                measurement="cumulative",
                unit="unique_people",
                sex="male",
            ),
            "pos_fem": CMU(
                category="pcr_tests_positive",
                measurement="cumulative",
                unit="unique_people",
                sex="female",
            ),
            "pos_oth": CMU(
                category="pcr_tests_positive",
                measurement="cumulative",
                unit="unique_people",
                sex="other",
            ),
            # # deaths by sex
            "dths_male": CMU(
                category="deaths", measurement="cumulative", unit="people", sex="male"
            ),
            "dths_fem": CMU(
                category="deaths", measurement="cumulative", unit="people", sex="female"
            ),
            "dths_oth": CMU(
                category="deaths", measurement="cumulative", unit="people", sex="other"
            ),
            # #
            # # age breakdowns
            # #
            # # positive cases by age
            "pos_0_9": CMU(
                category="pcr_tests_positive",
                measurement="cumulative",
                unit="unique_people",
                age="0-9",
            ),
            "pos_10_19": CMU(
                category="pcr_tests_positive",
                measurement="cumulative",
                unit="unique_people",
                age="10-19",
            ),
            "pos_20_29": CMU(
                category="pcr_tests_positive",
                measurement="cumulative",
                unit="unique_people",
                age="20-29",
            ),
            "pos_30_39": CMU(
                category="pcr_tests_positive",
                measurement="cumulative",
                unit="unique_people",
                age="30-39",
            ),
            "pos_40_49": CMU(
                category="pcr_tests_positive",
                measurement="cumulative",
                unit="unique_people",
                age="40-49",
            ),
            "pos_50_59": CMU(
                category="pcr_tests_positive",
                measurement="cumulative",
                unit="unique_people",
                age="50-59",
            ),
            "pos_60_69": CMU(
                category="pcr_tests_positive",
                measurement="cumulative",
                unit="unique_people",
                age="60-69",
            ),
            "pos_70_79": CMU(
                category="pcr_tests_positive",
                measurement="cumulative",
                unit="unique_people",
                age="70-79",
            ),
            "pos_80_89": CMU(
                category="pcr_tests_positive",
                measurement="cumulative",
                unit="unique_people",
                age="80-89",
            ),
            "pos_90": CMU(
                category="pcr_tests_positive",
                measurement="cumulative",
                unit="unique_people",
                age="90_plus",
            ),
            # # deaths by age
            "dths_0_9": CMU(
                category="deaths", measurement="cumulative", unit="people", age="0-9"
            ),
            "dths_10_19": CMU(
                category="deaths", measurement="cumulative", unit="people", age="10-19"
            ),
            "dths_20_29": CMU(
                category="deaths", measurement="cumulative", unit="people", age="20-29"
            ),
            "dths_30_39": CMU(
                category="deaths", measurement="cumulative", unit="people", age="30-39"
            ),
            "dths_40_49": CMU(
                category="deaths", measurement="cumulative", unit="people", age="40-49"
            ),
            "dths_50_59": CMU(
                category="deaths", measurement="cumulative", unit="people", age="50-59"
            ),
            "dths_60_69": CMU(
                category="deaths", measurement="cumulative", unit="people", age="60-69"
            ),
            "dths_70_79": CMU(
                category="deaths", measurement="cumulative", unit="people", age="70-79"
            ),
            "dths_80_89": CMU(
                category="deaths", measurement="cumulative", unit="people", age="80-89"
            ),
            "dths_90": CMU(
                category="deaths",
                measurement="cumulative",
                unit="people",
                age="90_plus",
            ),
            # # hospitalizations by age
            "ip_y_0_9": CMU(
                category="hospital_beds_in_use_covid",
                measurement="cumulative",
                unit="people",
                age="0-9",
            ),
            "ip_y_10_19": CMU(
                category="hospital_beds_in_use_covid",
                measurement="cumulative",
                unit="people",
                age="10-19",
            ),
            "ip_y_20_29": CMU(
                category="hospital_beds_in_use_covid",
                measurement="cumulative",
                unit="people",
                age="20-29",
            ),
            "ip_y_30_39": CMU(
                category="hospital_beds_in_use_covid",
                measurement="cumulative",
                unit="people",
                age="30-39",
            ),
            "ip_y_40_49": CMU(
                category="hospital_beds_in_use_covid",
                measurement="cumulative",
                unit="people",
                age="40-49",
            ),
            "ip_y_50_59": CMU(
                category="hospital_beds_in_use_covid",
                measurement="cumulative",
                unit="people",
                age="50-59",
            ),
            "ip_y_60_69": CMU(
                category="hospital_beds_in_use_covid",
                measurement="cumulative",
                unit="people",
                age="60-69",
            ),
            "ip_y_70_79": CMU(
                category="hospital_beds_in_use_covid",
                measurement="cumulative",
                unit="people",
                age="70-79",
            ),
            "ip_y_80_89": CMU(
                category="hospital_beds_in_use_covid",
                measurement="cumulative",
                unit="people",
                age="80-89",
            ),
            "ip_y_90": CMU(
                category="hospital_beds_in_use_covid",
                measurement="cumulative",
                unit="people",
                age="90_plus",
            ),
            # # icu admissions by age
            "ic_y_0_9": CMU(
                category="icu_beds_in_use_covid",
                measurement="cumulative",
                unit="people",
                age="0-9",
            ),
            "ic_y_10_19": CMU(
                category="icu_beds_in_use_covid",
                measurement="cumulative",
                unit="people",
                age="10-19",
            ),
            "ic_y_20_29": CMU(
                category="icu_beds_in_use_covid",
                measurement="cumulative",
                unit="people",
                age="20-29",
            ),
            "ic_y_30_39": CMU(
                category="icu_beds_in_use_covid",
                measurement="cumulative",
                unit="people",
                age="30-39",
            ),
            "ic_y_40_49": CMU(
                category="icu_beds_in_use_covid",
                measurement="cumulative",
                unit="people",
                age="40-49",
            ),
            "ic_y_50_59": CMU(
                category="icu_beds_in_use_covid",
                measurement="cumulative",
                unit="people",
                age="50-59",
            ),
            "ic_y_60_69": CMU(
                category="icu_beds_in_use_covid",
                measurement="cumulative",
                unit="people",
                age="60-69",
            ),
            "ic_y_70_79": CMU(
                category="icu_beds_in_use_covid",
                measurement="cumulative",
                unit="people",
                age="70-79",
            ),
            "ic_y_80_89": CMU(
                category="icu_beds_in_use_covid",
                measurement="cumulative",
                unit="people",
                age="80-89",
            ),
            "ic_y_90": CMU(
                category="icu_beds_in_use_covid",
                measurement="cumulative",
                unit="people",
                age="90_plus",
            ),
            # #
            # # race breakdowns
            # #
            # # positive cases by race
            "pos_aian": CMU(
                category="pcr_tests_positive",
                measurement="cumulative",
                unit="unique_people",
                race="ai_an",
            ),
            "pos_asn": CMU(
                category="pcr_tests_positive",
                measurement="cumulative",
                unit="unique_people",
                race="asian",
            ),
            "pos_blk": CMU(
                category="pcr_tests_positive",
                measurement="cumulative",
                unit="unique_people",
                race="black",
            ),
            "pos_wht": CMU(
                category="pcr_tests_positive",
                measurement="cumulative",
                unit="unique_people",
                race="white",
            ),
            "pos_mltoth": CMU(
                category="pcr_tests_positive",
                measurement="cumulative",
                unit="unique_people",
                race="multiple_other",
            ),
            "pos_unk": CMU(
                category="pcr_tests_positive",
                measurement="cumulative",
                unit="unique_people",
                race="unknown",
            ),
            # # deaths by race
            "dth_aian": CMU(
                category="deaths", measurement="cumulative", unit="people", race="ai_an"
            ),
            "dth_asn": CMU(
                category="deaths", measurement="cumulative", unit="people", race="asian"
            ),
            "dth_blk": CMU(
                category="deaths", measurement="cumulative", unit="people", race="black"
            ),
            "dth_wht": CMU(
                category="deaths", measurement="cumulative", unit="people", race="white"
            ),
            "dth_mltoth": CMU(
                category="deaths",
                measurement="cumulative",
                unit="people",
                race="multiple_other",
            ),
            "dth_unk": CMU(
                category="deaths",
                measurement="cumulative",
                unit="people",
                race="unknown",
            ),
            # #
            # # ethnicity breakdowns
            # #
            # # positive cases by ethnicity
            "pos_e_hsp": CMU(
                category="pcr_tests_positive",
                measurement="cumulative",
                unit="unique_people",
                ethnicity="hispanic",
            ),
            "pos_e_nhsp": CMU(
                category="pcr_tests_positive",
                measurement="cumulative",
                unit="unique_people",
                ethnicity="non-hispanic",
            ),
            "pos_e_unk": CMU(
                category="pcr_tests_positive",
                measurement="cumulative",
                unit="unique_people",
                ethnicity="unknown",
            ),
            # # deaths by ethnicity
            "dth_e_hsp": CMU(
                category="deaths",
                measurement="cumulative",
                unit="people",
                ethnicity="hispanic",
            ),
            "dth_e_nhsp": CMU(
                category="deaths",
                measurement="cumulative",
                unit="people",
                ethnicity="non-hispanic",
            ),
            "dth_e_unk": CMU(
                category="deaths",
                measurement="cumulative",
                unit="people",
                ethnicity="unknown",
            ),
        }
        out = (
            df.melt(id_vars=["location"], value_vars=crename.keys())
            .assign(
                dt=self._retrieve_dt("US/Central"), vintage=self._retrieve_vintage()
            )
            .dropna()
        )
        out.loc[:, "value"] = pd.to_numeric(out["value"])

        # Extract category information and add other variable context
        out = self.extract_CMU(out, crename)

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
