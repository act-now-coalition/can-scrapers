import pandas as pd
import requests
DC_STATEHOOD=1 #Include DC in the us/fips library
import us

from can_tools.scrapers.base import CMU
from can_tools.scrapers.official.base import FederalDashboard, DatasetBase

"""
NOTES: 
    currently does not track "TOTAL" column in moderna/pfizer datasets ---- not sure how to do so—brainstorm then ask lol
    think about how to add weekly allocations/amounts for pfizer/moderna datasets

    some not tallied by region but department (eg: federal entities) -- how to include?

    normally i would make pfizer and moderna just different objects of same class
        but idk if that would work with the setup we have
"""
class CDCVaccineBase(FederalDashboard, DatasetBase):
    has_location = True
    location_type = "state"
    provider = "cdc"
    source: "string"
    query_type: "string"
    crename: dict

    def fetch(self):
        fetch_urls = {
            'moderna': "https://data.cdc.gov/resource/b7pe-5nws.json", 
            'pfizer': "https://data.cdc.gov/resource/saz5-9hgg.json",
            'total': "https://covid.cdc.gov/covid-data-tracker/COVIDData/getAjaxData?id=vaccination_data"
        } 
        res = requests.get(fetch_urls[self.query_type])

        if not res.ok:
            raise ValueError("could not complete request from url source")
        return res    

    def normalize(self, data):
        raw = data.json()
        df = pd.json_normalize(raw).rename(columns={"jurisdiction":"location"})
        
        #fix column names to match us library convention & remove extra chars
        df['location'] = df['location'].str.replace('*','').str.replace(' ~','')
        fix_names = {"U.S. Virgin Islands":"Virgin Islands"}
        df['location'] = df['location'].map(fix_names).fillna(df['location'])
        
        #use when dataset was last updated as date
        url_time = data.headers["Last-Modified"]
        df["dt"] = pd.to_datetime(url_time, format='%a, %d %b %Y %H:%M:%S GMT').date()
        
        #replace location names w/ fips codes; keep only locations that have a fips code
        df = self._replace_fips(df)
        #melt into correct format and return
        return self._reshape(df)

    def _reshape(self, data):
        """
        melt data into format for put() function ()
        add comment....
        """

        out = data.melt(id_vars=["dt", "location", "loc_name"], value_vars=self.crename.keys()).dropna()
        out = self.extract_CMU(out, self.crename)
        out["vintage"] = self._retrieve_vintage()
        if out["value"].dtype == object:
            out["value"] = out["value"].str.replace(',', '').str.replace('N/A', '0').astype(int)

        cols_to_keep = [
            "vintage",
            "dt",
            "location",
            "loc_name",
            "category",
            "measurement",
            "unit",
            "age",
            "race",
            "ethnicity",
            "sex",
            "value"
        ]
        return out.loc[:, cols_to_keep]

    def _replace_fips(self, data):
        """
        replace state names with fips codes and remove entries w/o a fips code
        """
        data["loc_name"] = data["location"] #for debugging/viewing
        data['location'] = data['location'].map(us.states.mapping('name', 'fips'))
        return data.dropna().reset_index(drop=True)

class CDCVaccineTotal(CDCVaccineBase):
    query_type = 'total'
    source = "https://covid.cdc.gov/covid-data-tracker/#vaccinations"
    crename = { 
        "Doses_Distributed": CMU(
            category="vaccine_distributed", measurement="cumulative", unit="doses"
        ),
        "Doses_Administered": CMU(
            category="vaccine_initiated", measurement="cumulative", unit="people"
        ),
    }

    #override base method
    def normalize(self, data):
        data = data.json()
        df = pd.json_normalize(data['vaccination_data']).rename(columns={"Date": "dt", "LongName": "location"})
                
        #fix column name formatting to match us library convention
        fix_names = {"New York State": "New York"} 
        df["location"] = df['location'].map(fix_names).fillna(df['location'])
        df["loc_name"] = df["location"] #for debugging/viewing

        #replace location names w/ fips codes, and keep only locations that have a fips code
        df = self._replace_fips(df)
        #and melt + return
        return self._reshape(df)


class CDCVaccinePfizer(CDCVaccineBase):
    query_type = 'pfizer'
    source = "https://data.cdc.gov/Vaccinations/COVID-19-Vaccine-Distribution-Allocations-by-Juris/saz5-9hgg"
    crename = {
            "total_pfizer_allocation_first_dose_shipments": CMU(
                category="pfizer_vaccine_first_dose_allocated", measurement="cumulative", unit="doses"
            ),
            "total_allocation_pfizer_second_dose_shipments": CMU(
                category="pfizer_vaccine_second_dose_allocated", measurement="cumulative", unit="doses"
            ),
        }


class CDCVaccineModerna(CDCVaccineBase):
    query_type = 'moderna'
    source = "https://data.cdc.gov/Vaccinations/COVID-19-Vaccine-Distribution-Allocations-by-Juris/b7pe-5nws"
    crename = {
            "total_moderna_allocation_first_dose_shipments": CMU(
                category="moderna_vaccine_first_dose_allocated", measurement="cumulative", unit="doses"
            ),
            "total_allocation_moderna_second_dose_shipments": CMU(
                category="moderna_vaccine_second_dose_allocated", measurement="cumulative", unit="doses"
            ),
        }
    