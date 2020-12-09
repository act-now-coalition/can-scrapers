import pandas as pd

from can_tools.scrapers.base import CMU
from can_tools.scrapers.official.federal.HHS.common import HHSDataset


class HHSInpatientTimeSeries(HHSDataset):
    has_location = True
    location_type = "state"
    source = (
        "https://healthdata.gov/dataset/"
        "covid-19-estimated-patient-impact-and-hospital-capacity-state"
    )

    def fetch(self):
        return self.search_homepage_for_download("inpatient_all")

    def normalize(self, data: str) -> pd.DataFrame:
        crename = {
            "inpatient beds occupied estimated": CMU(
                category="hospital_beds_in_use", measurement="current", unit="beds"
            ),
            "percentage of inpatient beds occupied estimated": CMU(
                category="hospital_beds_in_use",
                measurement="current",
                unit="percentage",
            ),
            "total inpatient beds": CMU(
                category="hospital_beds_capacity", measurement="current", unit="beds"
            ),
        }

        return self.normalize_helper(data, crename)


class HHSInpatientCovidTimeSeries(HHSDataset):
    has_location = True
    location_type = "state"
    source = (
        "https://healthdata.gov/dataset/"
        "covid-19-estimated-patient-impact-and-hospital-capacity-state"
    )

    def fetch(self):
        return self.search_homepage_for_download("inpatient_covid")

    def normalize(self, data: str) -> pd.DataFrame:
        crename = {
            "inpatient beds occupied by covid-19 patients estimated": CMU(
                category="hospital_beds_in_use_covid",
                measurement="current",
                unit="beds",
            ),
            "percentage of inpatient beds occupied by covid-19 patients estimated": CMU(
                category="hospital_beds_in_use_covid",
                measurement="current",
                unit="percentage",
            ),
        }

        return self.normalize_helper(data, crename)


class HHSICUTimeSeries(HHSDataset):
    has_location = True
    location_type = "state"
    source = (
        "https://healthdata.gov/dataset/"
        "covid-19-estimated-patient-impact-and-hospital-capacity-state"
    )

    def fetch(self):
        return self.search_homepage_for_download("icu")

    def normalize(self, data: str) -> pd.DataFrame:
        crename = {
            "staffed adult icu beds occupied estimated": CMU(
                category="adult_icu_beds_in_use", measurement="current", unit="beds"
            ),
            "percentage of staffed adult icu beds occupied estimated": CMU(
                category="adult_icu_beds_in_use",
                measurement="current",
                unit="percentage",
            ),
            "total staffed adult icu beds": CMU(
                category="adult_icu_beds_capacity", measurement="current", unit="beds"
            ),
        }

        return self.normalize_helper(data, crename)
