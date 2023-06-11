import pandas as pd
import us

from can_tools.scrapers.base import CMU
from can_tools.scrapers.official.federal.HHS.common import HHSDataset


class HHSReportedPatientImpactHospitalCapacityState(HHSDataset):
    has_location = True
    location_type = "state"
    source = (
        "https://healthdata.gov/Hospital/COVID-19-Reported-Patient-Impact-and-Hospital-Capa"
        "/g62h-syeh"
    )
    source_url = (
        "https://healthdata.gov/api/views/g62h-syeh/rows.csv?accessType=DOWNLOAD"
    )

    def fetch(self):
        df = pd.read_csv(self.source_url)
        return df

    def normalize(self, data: str) -> pd.DataFrame:
        df = data.copy()
        # Read the dataframe from the string csv
        df.columns = [x.lower().strip() for x in df.columns]

        # Set date and fips code
        df.loc[:, "dt"] = pd.to_datetime(df["date"])
        df.loc[:, "location"] = df["state"].map(lambda x: int(us.states.lookup(x).fips))

        # Combine adult and pediatric hospital admissions into one variable
        df["previous_day_admission_covid_confirmed"] = (
            df["previous_day_admission_adult_covid_confirmed"]
            + df["previous_day_admission_pediatric_covid_confirmed"]
        )

        crename = {
            "critical_staffing_shortage_today_yes": CMU(
                category="critical_staff_shortage_yes",
                measurement="current",
                unit="hospitals",
            ),
            "critical_staffing_shortage_today_no": CMU(
                category="critical_staff_shortage_no",
                measurement="current",
                unit="hospitals",
            ),
            "critical_staffing_shortage_today_yes": CMU(
                category="critical_staff_shortage_noreport",
                measurement="current",
                unit="hospitals",
            ),
            "critical_staffing_shortage_anticipated_within_week_yes": CMU(
                category="critical_staff_shortage_yes",
                measurement="anticipated_within_7_day",
                unit="hospitals",
            ),
            "critical_staffing_shortage_anticipated_within_week_no": CMU(
                category="critical_staff_shortage_no",
                measurement="anticipated_within_7_day",
                unit="hospitals",
            ),
            "critical_staffing_shortage_anticipated_within_week_yes": CMU(
                category="critical_staff_shortage_noreport",
                measurement="anticipated_within_7_day",
                unit="hospitals",
            ),
            "inpatient_beds": CMU(
                category="hospital_beds_capacity", measurement="current", unit="beds"
            ),
            "inpatient_beds_used": CMU(
                category="hospital_beds_in_use", measurement="current", unit="beds"
            ),
            "inpatient_beds_used_covid": CMU(
                category="hospital_beds_in_use_covid",
                measurement="current",
                unit="beds",
            ),
            "inpatient_beds_utilization": CMU(
                category="hospital_beds_in_use",
                measurement="current",
                unit="percentage",
            ),
            "total_staffed_adult_icu_beds": CMU(
                category="adult_icu_beds_capacity", measurement="current", unit="beds"
            ),
            "staffed_adult_icu_bed_occupancy": CMU(
                category="adult_icu_beds_in_use", measurement="current", unit="beds"
            ),
            "staffed_icu_adult_patients_confirmed_covid": CMU(
                category="adult_icu_beds_in_use_covid",
                measurement="current",
                unit="beds",
            ),
            "adult_icu_bed_covid_utilization": CMU(
                category="adult_icu_beds_in_use",
                measurement="current",
                unit="percentage",
            ),
            "previous_day_admission_covid_confirmed": CMU(
                category="hospital_admissions_covid",
                measurement="new",
                unit="people",
            ),
        }

        # Put into long form
        out: pd.DataFrame = df.melt(
            id_vars=["dt", "location"], value_vars=crename.keys()
        )
        out.loc[:, "value"] = pd.to_numeric(
            out["value"].astype(str).str.replace(",", "").replace("nan", None)
        )
        # Dropping missing values. As of 2023-06-11 all missing values are from
        # before 2020-08-06.
        out = out.dropna(subset=["value"])

        # Add category, measurement, unit, age, sex, race
        out = self.extract_CMU(out, crename)
        out["vintage"] = self._retrieve_vintage()

        return out
