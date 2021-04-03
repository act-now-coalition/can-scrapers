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
        "vaccine_initiated": CMU(
            category="total_vaccine_initiated",
            measurement="new",
            unit="people",
        ),

        "vaccine_completed": CMU(
            category="total_vaccine_completed",
            measurement="new",
            unit="people",
        ),
        "vaccine_initiated_sum": CMU(
            category="total_vaccine_initiated",
            measurement="cumulative",
            unit="people",
        ),

        "vaccine_completed_sum": CMU(
            category="total_vaccine_completed",
            measurement="cumulative",
            unit="people",
        )
    }

    def fetch(self):
        return self.get_all_jsons(self.service, 0, "1")

    def _fix_fips(self, df):
        out = df
        out.fips = str(self.state_fips) + out.fips
        out.fips = pd.to_numeric(out.fips, errors='coerce')
        return df

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

        # melt daily/new data to long form
        out = (
            totals.melt(id_vars=["fips", "dt"], value_vars=['vaccine_initiated','vaccine_completed'])
            .dropna()
        )
        
        # sum by county and variable type to get cumulative data, keep the most recent date
        cumulative = out.groupby(['fips','variable']).agg(
            {'value': 'sum', 'dt':'last'}
        ).reset_index()
        # label the data as cumulative
        cumulative['variable'] = cumulative['variable'] + '_sum'
       
        # combine new and cumulative data
        out = pd.concat([cumulative, out])
        out = (out.pipe(self.extract_CMU, cmu=self.variables)
            .assign(
                vintage=self._retrieve_vintage(),
                dt=out['dt'].dt.date
            )
            .drop(["variable"], axis=1)
        )
        return out.rename(columns={'fips':'location'})

class AlaskaVaccineDemographics(AlaskaCountyVaccine):

    variables = {
        "initiated": CMU(
            category="total_vaccine_initiated",
            measurement="new",
            unit="people",
        ),

        "completed": CMU(
            category="total_vaccine_completed",
            measurement="new",
            unit="people",
        ),
        "initiated_sum": variables.INITIATING_VACCINATIONS_ALL,
        "completed_sum": variables.FULLY_VACCINATED_ALL
    }

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
                .replace("U", "unknown")
                .replace('--1', ":initiated")
                .replace("--2", ":completed")
                .replace("--3", ":dose_unknown")
            for z in ["--".join(str(y) for y in x) for x in unstacked.columns.values]
        ]
        

        out = unstacked.reset_index()
        out = self._fix_fips(out)
        out = out.replace(np.nan, 0)
        return out

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
                .replace('--1', ":initiated")
                .replace("--2", ":completed")
                .replace("--3", ":dose_unknown")
            for z in ["--".join(str(y) for y in x) for x in unstacked.columns.values]
        ]
        

        out = unstacked.reset_index()
        out = self._fix_fips(out)
        out = out.replace(np.nan, 0)

        return out

    def normalize(self, data):
        df = self._rename_and_prep(data)
        sex = self._get_by_sex(df)
        age = self._get_by_age(df)

        sex_total = self._cumulate_combine_melt(sex, 'sex')
        age_total = self._cumulate_combine_melt(age, 'age')
        return pd.concat([sex_total, age_total])

        return out.rename(columns={'fips':'location'})

    def _cumulate_combine_melt(self, df, demo):
        """
        takes data and transforms into long form with CMU pairings, and calculates cumulative data from daily and returns both new/cumulative data
        accepts:
            df: dataframe -- df of new/daily data in wide form
            demo: str -- name of the demographic according to CMU class (sex,age,race etc)

        returns: df in long form with CMU columns and cumulative and new data
        """

        # use all cols except date and location as value vars
        val_vars = [e for e in list(df.columns) if e not in ('dt', 'fips')]
        out = (
            df.melt(id_vars=["fips", "dt"], value_vars=val_vars)
            .dropna()
        )

        # split variable column name (in form of demo:variable) into two columns
        out[['temp_demo', 'variable']] = out['variable'].str.split(":",expand=True)
        
        # remove unknown vals
        out = out[out['variable'] != 'dose_unknown']

        # calculate cumulative values and label as such
        cumulative = out.groupby(['fips','variable','temp_demo']).agg(
            {'value': 'sum', 'dt':'last'}
        ).reset_index()
        cumulative['variable'] = cumulative['variable'] + '_sum'
        
        # combine new and cumulative doses
        out = pd.concat([out,cumulative])
        out = (out.pipe(self.extract_CMU, cmu=self.variables)
            .assign(
                vintage=self._retrieve_vintage(),
                dt=out['dt'].dt.date
            )
            .drop(["variable"], axis=1)
        )
        out[demo] = out['temp_demo']
        return out.drop(columns={'temp_demo'})
