from io import StringIO

import pandas as pd
import us

from can_tools.scrapers.base import CMU
from can_tools.scrapers.official.federal.HHS.common import HHSDataset


class HHSReportedPatientImpactHospitalCapacity(HHSDataset):
    has_location = True
    location_type = "state"
    source = (
        "https://healthdata.gov/dataset/"
        "covid-19-reported-patient-impact-and-hospital-capacity-state-timeseries/"
    )
    dsid = "83b4a668-9321-4d8c-bc4f-2bef66c49050"

    def fetch(self):
        return self.search_homepage_for_download("reported_hospital_utilization")

    def normalize(self, data: str) -> pd.DataFrame:

        # Read the dataframe from the string csv
        df = pd.read_csv(StringIO(data))
        df.columns = [x.lower().strip() for x in df.columns]

        # Set date and fips code
        df.loc[:, "dt"] = pd.to_datetime(df["date"])
        df.loc[:, "location"] = df["state"].map(lambda x: int(us.states.lookup(x).fips))

        crename = {
            "critical_staffing_shortage_today_yes": CMU(
                category="critical_staff_shortage_yes", measurement="current", unit="hospitals"
            ),
            "critical_staffing_shortage_today_no": CMU(
                category="critical_staff_shortage_no", measurement="current", unit="hospitals"
            ),
            "critical_staffing_shortage_today_yes": CMU(
                category="critical_staff_shortage_noreport", measurement="current", unit="hospitals"
            ),
            "critical_staffing_shortage_anticipated_within_week_yes": CMU(
                category="critical_staff_shortage_yes", measurement="anticipated_within_7_day", unit="hospitals"
            ),
            "critical_staffing_shortage_anticipated_within_week_no": CMU(
                category="critical_staff_shortage_no", measurement="anticipated_within_7_day", unit="hospitals"
            ),
            "critical_staffing_shortage_anticipated_within_week_yes": CMU(
                category="critical_staff_shortage_noreport", measurement="anticipated_within_7_day", unit="hospitals"
            ),
            "inpatient_beds": CMU(
                category="hospital_beds_capacity", measurement="current", unit="beds"
            ),
            "inpatient_beds_used": CMU(
                category="hospital_beds_in_use", measurement="current", unit="beds"
            ),
            "inpatient_beds_used_covid": CMU(
                category="hospital_beds_in_use_covid", measurement="current", unit="beds"
            ),
            "inpatient_beds_utilization": CMU(
                category="hospital_beds_in_use", measurement="current", unit="percentage"
            ),
            "total_staffed_adult_icu_beds": CMU(
                category="adult_icu_beds_capacity", measurement="current", unit="beds"
            ),
            "staffed_adult_icu_bed_occupancy": CMU(
                category="adult_icu_beds_in_use", measurement="current", unit="beds"
            ),
            "staffed_icu_adult_patients_confirmed_covid": CMU(
                category="adult_icu_beds_in_use_covid", measurement="current", unit="beds"
            ),
            "adult_icu_bed_covid_utilization": CMU(
                category="adult_icu_beds_in_use", measurement="current", unit="percentage"
            )
        }

        # Put into long form
        out = df.melt(id_vars=["dt", "location"], value_vars=crename.keys())
        out.loc[:, "value"] = pd.to_numeric(
            out["value"].astype(str).str.replace(",", "").replace("nan", None)
        )

        # Add category, measurement, unit, age, sex, race
        out = self.extract_CMU(out, crename)
        out["vintage"] = self._retrieve_vintage()
        cols_2_keep = [
            "vintage",
            "dt",
            "location",
            "category",
            "measurement",
            "unit",
            "age",
            "sex",
            "race",
            "value",
        ]

        return out
