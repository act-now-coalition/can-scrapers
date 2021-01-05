import pandas as pd
import requests

from can_tools.scrapers.base import CMU
from can_tools.scrapers.official.base import FederalDashboard

class CDCVaccineBase(FederalDashboard):
    has_location = True
    location_type = "state"
    source: "string"
    provider = "cdc"
    query_type: "string"

    #for testing clarity
    def __init__(self):
        print('source: ' + self.query_type)

    def fetch(self):
        fetch_urls = {'moderna': "https://data.cdc.gov/resource/b7pe-5nws.json", 'pfizer': "https://data.cdc.gov/resource/saz5-9hgg.json",
        'total': "https://covid.cdc.gov/covid-data-tracker/COVIDData/getAjaxData?id=vaccination_data"} 
        res = requests.get(fetch_urls[self.query_type])

        if not res.ok:
            raise ValueError("wrong link lol")
        return res.json()    


class CDCVaccineTotal(CDCVaccineBase):
    query_type = 'total'
    source = "https://covid.cdc.gov/covid-data-tracker/#county-view"
    
    def normalize(self, data):
        return pd.json_normalize(data['vaccination_data'])


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