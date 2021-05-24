from abc import ABC, abstractmethod

import pandas as pd
import us

from can_tools.scrapers import CMU
from can_tools.scrapers.official.base import ArcGIS


def case_cmu(**kw):
    kwargs = dict(category="cases", measurement="cumulative", unit="people")
    kwargs.update(kw)
    return CMU(**kwargs)


def deaths_cmu(**kw):
    return case_cmu(category="deaths", **kw)


class WisconsinArcGIS(ArcGIS, ABC):
    """
    ArcGIS scraper that retrieves dashboard information for the
    state of Wisconsin (which has their own self-hosted ArcGIS
    instance)
    """

    has_location = True
    state_fips = int(us.states.lookup("Wisconsin").fips)
    source = "https://www.dhs.wisconsin.gov/covid-19/data.htm"
    source_name = "Wisconsin Department of Health Services"

    location_type: str
    SERVICE: str = "DHS_COVID19/COVID19_WI"
    SHEET: int

    crename = {
        "positive": CMU(
            category="cases",
            measurement="cumulative",
            unit="people",
        ),
        "negative": CMU(
            category="pcr_tests_negative",
            measurement="cumulative",
            unit="unique_people",
        ),
        "pos_new": CMU(
            category="cases",
            measurement="new",
            unit="people",
        ),
        "neg_new": CMU(
            category="pcr_tests_negative",
            measurement="new",
            unit="unique_people",
        ),
        "test_new": CMU(
            category="pcr_tests_total",
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
        # sex
        "pos_fem": case_cmu(sex="female"),
        "pos_male": case_cmu(sex="male"),
        "dths_fem": deaths_cmu(sex="female"),
        "dths_male": deaths_cmu(sex="male"),
        # age
        "pos_0_9": case_cmu(age="0-9"),
        "pos_10_19": case_cmu(age="10-19"),
        "pos_20_29": case_cmu(age="20-29"),
        "pos_30_39": case_cmu(age="30-39"),
        "pos_40_49": case_cmu(age="40-49"),
        "pos_50_59": case_cmu(age="50-59"),
        "pos_60_69": case_cmu(age="60-69"),
        "pos_70_79": case_cmu(age="70-79"),
        "pos_80_89": case_cmu(age="80-89"),
        "pos_90": case_cmu(age="90_plus"),
        "dths_0_9": deaths_cmu(age="0-9"),
        "dths_10_19": deaths_cmu(age="10-19"),
        "dths_20_29": deaths_cmu(age="20-29"),
        "dths_30_39": deaths_cmu(age="30-39"),
        "dths_40_49": deaths_cmu(age="40-49"),
        "dths_50_59": deaths_cmu(age="50-59"),
        "dths_60_69": deaths_cmu(age="60-69"),
        "dths_70_79": deaths_cmu(age="70-79"),
        "dths_80_89": deaths_cmu(age="80-89"),
        "dths_90": deaths_cmu(age="90_plus"),
        # race and ethnicity
        "pos_aian": case_cmu(race="ai_an"),
        "pos_asn": case_cmu(race="asian"),
        "pos_blk": case_cmu(race="black"),
        "pos_wht": case_cmu(race="white"),
        "pos_mltoth": case_cmu(race="multiple_other"),
        "pos_unk": case_cmu(race="unknown"),
        "pos_e_hsp": case_cmu(ethnicity="hispanic"),
        "pos_e_nhsp": case_cmu(ethnicity="non-hispanic"),
        "pos_e_unk": case_cmu(ethnicity="unknown"),
        "dths_aian": deaths_cmu(race="ai_an"),
        "dths_asn": deaths_cmu(race="asian"),
        "dths_blk": deaths_cmu(race="black"),
        "dths_wht": deaths_cmu(race="white"),
        "dths_mltoth": deaths_cmu(race="multiple_other"),
        "dths_unk": deaths_cmu(race="unknown"),
        "dths_e_hsp": deaths_cmu(ethnicity="hispanic"),
        "dths_e_nhsp": deaths_cmu(ethnicity="non-hispanic"),
        "dths_e_unk": deaths_cmu(ethnicity="unknown"),
    }

    @abstractmethod
    def get_location(self, df: pd.DataFrame):
        pass

    def fetch(self):
        return self.get_all_jsons(self.SERVICE, self.SHEET, "server")

    def arcgis_query_url(
        self,
        service="DHS_COVID19/COVID19_WI",
        sheet=1,
        srvid="server",
    ):
        out = f"https://dhsgis.wi.gov/{srvid}/rest/services/{service}/MapServer/{sheet}/query"

        return out

    def normalize(self, data):
        df = self.arcgis_jsons_to_df(data)
        df.columns = [x.lower() for x in list(df)]
        df["location"] = self.get_location(df)

        value_cols = list(set(df.columns) & set(self.crename.keys()))

        out = (
            df.melt(id_vars=["location"], value_vars=value_cols)
            .assign(
                dt=self._retrieve_dt("US/Central"), vintage=self._retrieve_vintage()
            )
            .dropna()
        )
        out.loc[:, "value"] = pd.to_numeric(out["value"])

        # Extract category information and add other variable context
        out = self.extract_CMU(out, self.crename)

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


class WisconsinCounties(WisconsinArcGIS):
    """
    Fetch county-level covid data from Wisconsin's ARCGIS dashboard
    """

    location_type = "county"
    SHEET = 1

    def get_location(self, df: pd.DataFrame):
        return df["geoid"].astype(int)


class WisconsinState(WisconsinArcGIS):
    """
    Fetch state-level covid data from Wisconsin's ARCGIS dashboard
    Includes demographic breakdowns
    """

    location_type = "state"
    SHEET = 3

    def get_location(self, df: pd.DataFrame):
        return self.state_fips
