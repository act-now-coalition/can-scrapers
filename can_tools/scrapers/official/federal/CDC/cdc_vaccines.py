import pandas as pd
import requests
import us

from can_tools.scrapers.base import CMU
from can_tools.scrapers.official.base import FederalDashboard, DatasetBase

class CDCVaccineBase(FederalDashboard, DatasetBase):
    has_location = True
    location_type = "state"
    provider = "cdc"
    source: "string"
    query_type: "string"

    def fetch(self):
        fetch_urls = {'moderna': "https://data.cdc.gov/resource/b7pe-5nws.json", 'pfizer': "https://data.cdc.gov/resource/saz5-9hgg.json",
        'total': "https://covid.cdc.gov/covid-data-tracker/COVIDData/getAjaxData?id=vaccination_data"} 
        res = requests.get(fetch_urls[self.query_type])

        if not res.ok:
            raise ValueError("wrong link lol")
        return res.json()    


class CDCVaccineTotal(CDCVaccineBase):
    query_type = 'total'
    source = "https://covid.cdc.gov/covid-data-tracker/#vaccinations"
    
    def _get_fips(self, names):
        """
            takes in a list of state names and returns a dictionary containing the fips codes for each respective state.
            ultimately used to help replace state names with fips code values
            
            Accepts
            -------
            names: list 
            list of state/territory names (must match names used in the us library)

            Returns
            -------
            map: dictionary
            dictionary containing each state name (key) and fips code (value)
        """
        map = {}
        for state in names: 
            if state in str(us.states.STATES_AND_TERRITORIES) or state == "DC":
                map[state] = us.states.lookup(state).fips
        return map

    def normalize(self, data):

        crename = { 
            "Doses_Distributed": CMU(
                category="vaccine_distributed", measurement="cumulative", unit="doses"
            ),
            # "Doses_Administered": CMU(
            #     category="people_initiating_vaccine", measurement="cumulative", unit="people"
            # ),
        }

        df = pd.json_normalize(data['vaccination_data']).rename(columns={"Date": "dt", "LongName": "location"})
        
        ## fetch FIPS codes for each state/territory and substitute for state/terr name
        locs = []
        fix_names = {"New York State": "New York", "District of Columbia": "DC"} 
        ## create a list of all states/territories, rename as needed (both in orig df and the list)
        ## list is then passed to _get_fips
        for loc in df["location"].unique():
            #rename entries that don't match us docs
            if loc in list(fix_names.keys()):
                loc = fix_names.get(loc)
                df['location'] = df['location'].map(fix_names).fillna(df['location'])
            locs.append(loc)
        fips_dict = self._get_fips(locs)

        #replace location name string with fips code
        #this (intentionally) removes all entries that don't have a fips code (ex: "Bureau of Prisons")
        df["loc_name"] = df["location"]
        df['location'] = df['location'].map(fips_dict)
        df = df.dropna().reset_index(drop=True)

        out = df.melt(id_vars=["dt", "location", "loc_name"], value_vars=crename.keys()).dropna()
        out = self.extract_CMU(out, crename)
        out["vintage"] = self._retrieve_vintage()
        out["value"] = out["value"].astype(int)

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
            "sex",
            "value"
        ]
        return out.loc[:, cols_to_keep]



class CDCVaccinePfizer(CDCVaccineBase):
    query_type = 'pfizer'
    source = "https://data.cdc.gov/Vaccinations/COVID-19-Vaccine-Distribution-Allocations-by-Juris/saz5-9hgg"

    def normalize(self, data):
        return pd.json_normalize(data)


class CDCVaccineModerna(CDCVaccineBase):
    query_type = 'moderna'
    source = "https://data.cdc.gov/Vaccinations/COVID-19-Vaccine-Distribution-Allocations-by-Juris/b7pe-5nws"
    
    def normalize(self, data):
        return pd.json_normalize(data)