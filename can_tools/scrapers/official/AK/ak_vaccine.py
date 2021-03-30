import pandas as pd
import us
import numpy as np

from can_tools.scrapers.official.base import ArcGIS
from can_tools.scrapers import CMU, variables


class AlaskaCountyVaccine(ArcGIS):
    has_location = False
    location_type = "county"
    source = "https://experience.arcgis.com/experience/c2ef4a4fcbe5458fbf2e48a21e4fece9"
    source_name = "The Arkansas Department of Health"
    service = "Vaccine_Dashboard_PROD"
    ARCGIS_ID = "WzFsmainVTuD5KML"
    state_fips = int(us.states.lookup("Alaska").fips)

    variables = {
        "VAC_DEMO_HAA_dose_1_total": variables.INITIATING_VACCINATIONS_ALL,
        "VAC_DEMO_HAA_dose_2_total": variables.FULLY_VACCINATED_ALL,
    }

    def fetch(self):
        return self.get_all_jsons(self.service, 0, "1")

    def _get_total_doses_administered_by_day(self, df):
        keep = df[["dt", "dose_num", "fips", "primarykeyid"]]
        count_per_day = (
            keep.groupby(["dt", "fips", "dose_num"])
            .count()
            .rename(columns={"primarykeyid": "count"})
        )
        initiated = self._get_total_initiated_by_day(count_per_day)
        completed = self._get_total_completed_by_day(count_per_day)
        # Don't know what dose_num = 3 means...?
        other = (
            count_per_day[count_per_day.index.get_level_values("dose_num") == 3]
            .rename(columns={"count": "vaccine_other"})
            .reset_index()
            .set_index(["dt", "fips"])
        )

        out = (
            initiated.join(completed["vaccine_completed"])
            .join(other["vaccine_other"])
            .drop(columns="dose_num")
            .replace(np.nan, 0)
            .reset_index()
        )
        out = self._fix_fips(out)
        return out.dropna()
    
    def _get_by_sex(self, df):
        keep = df[["dt", "dose_num", "fips", "sex", 'primarykeyid']]
        count_per_day = (
            keep.groupby(["dt", "fips", "dose_num", 'sex'])
            .count()
            .rename(columns={"primarykeyid": "count"})
        )
        unstacked = count_per_day.unstack().unstack()
        # Fix column names
        unstacked.columns = [
            z.replace("count--", "")
                .replace('F', 'female')
                .replace("M", "male")
                .replace("U", "gender_unknown")
                .replace('--1', "-initiated")
                .replace("--2", "-completed")
                .replace("--3", "-dose_unknown")
            for z in ["--".join(str(y) for y in x) for x in unstacked.columns.values]
        ]
        

        out = unstacked.reset_index()
        out = self._fix_fips(out)
        out = out.replace(np.nan, 0)
        return out
       
    def _fix_fips(self, df):
        out = df
        out.fips = str(self.state_fips) + out.fips
        out.fips = pd.to_numeric(out.fips, errors='coerce')
        return df


    def _get_by_age(self, df):
        keep = df[["dt", "dose_num", "fips", "age_bracket", 'primarykeyid']]
        count_per_day = (
            keep.groupby(["dt", "fips", "dose_num", 'age_bracket'])
            .count()
            .rename(columns={"primarykeyid": "count"})
        )
        unstacked = count_per_day.unstack().unstack()
        # Fix column names
        unstacked.columns = [
            z.replace("count--", "")
                .replace('Age Bracket - ', '')
                .replace('--1', "-initiated")
                .replace("--2", "-completed")
                .replace("--3", "-dose_unknown")
            for z in ["--".join(str(y) for y in x) for x in unstacked.columns.values]
        ]
        

        out = unstacked.reset_index()
        out = self._fix_fips(out)
        out = out.replace(np.nan, 0)

        return out

    def _get_total_initiated_by_day(self, df):
        # Get a cumsum of all rows where dose_num == 1
        initiated = (
            df[df.index.get_level_values("dose_num") == 1]
            .rename(columns={"count": "vaccine_initiated"})
            .reset_index()
            .set_index(["dt", "fips"])
        )
        return initiated

    def _get_total_completed_by_day(self, df):
        # Get a cumsum of all rows where dose_num == 2
        completed = (
            df[df.index.get_level_values("dose_num") == 2]
            .rename(columns={"count": "vaccine_completed"})
            .reset_index()
            .set_index(["dt", "fips"])
        )
        return completed

    def _rename_and_prep(self, data):
        df = self.arcgis_jsons_to_df(data)
        df = df.rename(
            columns={
                "RECIP_ID": "recipient_id",
                "RECIP_SEX": "sex",
                "RECIP_ETHNICITY": "ethnicity",
                "DATA_DT": "dt",
                "VAX_SERIES_COMPLETE": "fully_vaccinated",
                "REGION_NUM": "region",
                "BOROUGH_FIPS": "fips"
            }
        )
        df.columns = df.columns.str.lower()
        df.dt = pd.to_datetime(df.dt, unit="ms", origin="unix")

        return df

    def normalize(self, data):
        df = self._rename_and_prep(data)

        totals = self._get_total_doses_administered_by_day(df)
        sex = self._get_by_sex(df)
        age = self._get_by_age(df)

        out = totals.set_index(['dt', 'fips']).join(sex.set_index(['dt', 'fips'])).join(age.set_index(['dt', 'fips'])).reset_index()
        return out
