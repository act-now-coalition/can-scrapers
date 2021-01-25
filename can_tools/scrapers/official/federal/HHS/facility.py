from io import StringIO
from datetime import timedelta

import pandas as pd
import numpy as np
import us

from can_tools.scrapers.base import CMU
from can_tools.scrapers.official.federal.HHS.common import HHSDataset


class HHSReportedPatientImpactHospitalCapacityFacility(HHSDataset):
    has_location = True
    location_type = "multiple"
    source = (
        "https://healthdata.gov/dataset/"
        "covid-19-reported-patient-impact-and-hospital-capacity-facility"
    )
    dsid = "d475cc4e-83cd-4c16-be57-9105f300e0bc"

    def fetch(self):
        return self.dataset_details()

    def normalize(self, data: str) -> pd.DataFrame:
        # Read the dataframe from the string csv
        df = pd.read_csv(StringIO(data))
        df.columns = [x.lower().strip() for x in df.columns]

        # Set date and fips code
        # NOTE: collection_week refers to the first day of the week, so add 6
        # days to get the last day.
        df.loc[:, "dt"] = pd.to_datetime(df["collection_week"]) + timedelta(days=6)

        # Filter out all of the columns without a fips code for now -- I
        # think that it is likely that we could reverse engineer these
        # either by looking them up or by mapping city to county
        df = df.loc[~df["fips_code"].isna(), :]
        # :see_no_evil:
        df["location"] = (
            df["fips_code"]
            .astype(int)
            .replace(
                {
                    # 02120 corresponded to Kenai-Cook Inlet Division... It was
                    # then the relevant piece became Kenai Peninsula Borough which
                    # is 02122
                    # https://data.nber.org/asg/ASG_release/County_City/FIPS/FIPS_Changes.pdf
                    2120: 2122,
                    # City associated with the hospital is Seward which is in the
                    # Kenai Borough which is 02122 but I have no idea how this
                    # ended up with fips code 02210???
                    # https://en.wikipedia.org/wiki/Seward,_Alaska
                    2210: 2122,
                    # 02260 was fips code for Valdez-Chitina-Whittier Division... It
                    # was then put into Valdez–Cordova Census Area which is
                    # 02261, but 02261 was split in Jan 2019 and we'll need to change
                    # this again if we update geographies
                    # https://data.nber.org/asg/ASG_release/County_City/FIPS/FIPS_Changes.pdf
                    2260: 2261,
                    # 02280 corresponded to Wrangell-Petersburg but became the
                    # Petersburg Borough 02195 in 2012
                    # https://www.cdc.gov/nchs/nvss/bridged_race/county_geography-_changes2015.pdf
                    2280: 2195,
                    # City associated with the hospital is Cordova which is in the
                    # Valdez-Cordova census area but I don't know which one this
                    # ended up in after the split...
                    # https://en.wikipedia.org/wiki/Cordova,_Alaska
                    2080: 2261,
                }
            )
        )

        # Set all missing values (-999999) to nan for all numeric columns
        numeric_cols = list(df.select_dtypes("number"))
        df.loc[:, numeric_cols] = df.loc[:, numeric_cols].where(lambda x: x > 0, np.nan)

        # Variables that can be determined with "simple average"
        vars_to_compute_avg = [
            "inpatient_beds_7_day",
            "inpatient_beds_used_7_day",
            "total_staffed_adult_icu_beds_7_day",
            "staffed_adult_icu_bed_occupancy_7_day",
            "staffed_icu_adult_patients_confirmed_covid_7_day",
        ]
        for var in vars_to_compute_avg:
            df.loc[:, f"{var}_canavg"] = df.eval(f"{var}_sum / {var}_coverage")

        # Variables that require "more complicated average"
        aps = "total_adult_patients_hospitalized_confirmed_covid_7_day_sum"
        apc = "total_adult_patients_hospitalized_confirmed_covid_7_day_coverage"
        pps = "total_pediatric_patients_hospitalized_confirmed_covid_7_day_sum"
        ppc = "total_pediatric_patients_hospitalized_confirmed_covid_7_day_coverage"
        temp = df.eval(f"{aps} / {apc}")
        # Do the pediatric sum second so that we keep adult values if they're available
        # (while filling pediatric missing data with 0s) but if adult is missing then
        # it will stay as missing
        temp = temp + df.eval(f"{pps} / {ppc}").fillna(0.0)
        df.loc[:, "inpatient_beds_used_covid_7_day_canavg"] = temp.values

        crename = {
            "inpatient_beds_7_day_canavg": CMU(
                category="hospital_beds_capacity",
                measurement="rolling_average_7_day",
                unit="beds",
            ),
            "inpatient_beds_used_7_day_canavg": CMU(
                category="hospital_beds_in_use",
                measurement="rolling_average_7_day",
                unit="beds",
            ),
            # This column is generated by summing adult and pediatric
            # beds -- Should be missing if either is missing
            "inpatient_beds_used_covid_7_day_canavg": CMU(
                category="hospital_beds_in_use_covid",
                measurement="rolling_average_7_day",
                unit="beds",
            ),
            "total_staffed_adult_icu_beds_7_day_canavg": CMU(
                category="adult_icu_beds_capacity",
                measurement="rolling_average_7_day",
                unit="beds",
            ),
            "staffed_adult_icu_bed_occupancy_7_day_canavg": CMU(
                category="adult_icu_beds_in_use",
                measurement="rolling_average_7_day",
                unit="beds",
            ),
            "staffed_icu_adult_patients_confirmed_covid_7_day_canavg": CMU(
                category="adult_icu_beds_in_use_covid",
                measurement="rolling_average_7_day",
                unit="beds",
            ),
        }

        # Reshape by putting into long form
        df_long = df.melt(
            id_vars=["dt", "location"], value_vars=crename.keys()
        ).dropna()
        df_long.loc[:, "value"] = pd.to_numeric(
            df_long["value"].astype(str).str.replace(",", "")
        )

        # Add category, measurement, unit, age, sex, race
        df_long = self.extract_CMU(df_long, crename)

        # Group by relevant factors and sum
        identifier = [
            "dt",
            "location",
            "category",
            "measurement",
            "unit",
            "age",
            "sex",
            "race",
            "ethnicity",
        ]

        # TODO: We could do a different groupby and put this into states
        # or hospital regions
        out_county = (
            df_long.groupby(identifier)["value"]
            .agg(pd.Series.sum, skipna=False)
            .reset_index()
        )

        # TODO: Throwing out territories because I don't remember which weren't
        # included in the census data :(
        out_county = out_county.query("location < 60_000").copy()

        # Add vintage
        out_county.loc[:, "vintage"] = self._retrieve_vintage()
        out_county.loc[:, "location_type"] = "county"
        cols_2_keep = identifier + ["vintage", "location_type", "value"]

        return out_county.loc[:, cols_2_keep]
