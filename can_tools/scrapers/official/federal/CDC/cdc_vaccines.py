import pandas as pd
import requests
from abc import ABC
import us

from can_tools.scrapers.base import CMU
from can_tools.scrapers.official.base import FederalDashboard

"""
NOTES: 
    currently does not track "TOTAL" column in moderna/pfizer datasets ---- not sure how to do so—brainstorm then ask lol
    think about how to add weekly allocations/amounts for pfizer/moderna datasets

    some not tallied by region but department (eg: federal entities) -- how to include?

    normally i would make pfizer and moderna just different objects of same class
        but idk if that would work with the setup we have
"""

class CDCVaccineBase(FederalDashboard):
    has_location = True
    location_type = "state"
    provider = "cdc"
    source: "string"
    query_type: "string"
    crename: dict

    def fetch(self):
        fetch_urls = {
            "moderna": "https://data.cdc.gov/resource/b7pe-5nws.json",
            "pfizer": "https://data.cdc.gov/resource/saz5-9hgg.json",
            "total": "https://covid.cdc.gov/covid-data-tracker/COVIDData/getAjaxData?id=vaccination_data",
        }
        res = requests.get(fetch_urls[self.query_type])

        if not res.ok:
            raise ValueError("could not complete request from url source")
        return res

    def normalize(self, data):
        raw = data.json()
        df = pd.json_normalize(raw).rename(columns={"jurisdiction": "location"})

        # fix column names to match us library convention & remove extra chars
        df["location"] = df["location"].str.replace("*", "").str.replace(" ~", "")
        fix_names = {"U.S. Virgin Islands": "Virgin Islands"}
        df["location"] = df["location"].map(fix_names).fillna(df["location"])

        # print(df.head(20))

        # use when dataset was last updated as date
        url_time = data.headers["Last-Modified"]
        df["dt"] = pd.to_datetime(url_time, format="%a, %d %b %Y %H:%M:%S GMT").date()

        # replace location names w/ fips codes; keep only locations that have a fips code
        df["location"] = df["location"].map(us.states.mapping("name", "fips"))
        df = df.dropna().reset_index(drop=True)
        # melt into correct format and return
        return self._reshape(df)

    def _reshape(self, data):
        """
        melt data into format for put() function ()
        add comment....
        """
        #use these columns for melt (replaces crename.keys())
        #removes cols we dont want
        colnames = list(data.columns)
        colnames = [e for e in colnames if e not in {'hhs_region', 'dt', 'location', 'first_doses_12_14', 'second_doses_shipment_12_14','second_dose_shipment_week_of_01_18','doses_distribution_week_of_01_18'}]

        print(colnames)

        out = data.melt(
            id_vars=["dt", "location"], value_vars=colnames
        ).dropna()

        out['dt_str'] = out['variable']
        #remove date from col to make CMU variable 
        out.loc[~out['variable'].str.contains('total'), 'variable'] = out['variable'].str.strip().str[:-6]
        #rename slightly not matching colnames
        out.loc[out['variable'] == 'second_dose_shipment', 'variable'] = 'second_dose_shipment_week_of'

        # print("new variables: ")
        # for val in out['variable'].unique():
        #     print(val)

        #locate rows where entry is not total, take the last 5 chars
        #mark rows with total as keep (to keep old time)
        out.loc[out['dt_str'].str.contains('total'), 'dt_str'] = 'keep'
        out.loc[~out['dt_str'].str.contains('total'), 'dt_str'] = out['dt_str'].str.strip().str[-5:]
        out['dt_str'] = pd.to_datetime(out.dt_str[out['dt_str'] != 'keep'], format="%m_%d")

        out['dt_str'] = out['dt_str'].mask(out['dt_str'].dt.month == 12, 
                             out['dt_str'] + pd.offsets.DateOffset(year=2020))
        out['dt_str'] = out['dt_str'].mask(out['dt_str'].dt.month == 1, 
                             out['dt_str'] + pd.offsets.DateOffset(year=2021))

        print(out)

        #replace dt for non total rows with dt_str data
        out.loc[out["dt_str"].notnull(), 'dt'] = out['dt_str']
        
        # print('dates: ')
        # for d in out['dt'].unique():
        #     print(d)

        out = self.extract_CMU(out, self.crename)
        out["vintage"] = self._retrieve_vintage()
        if out["value"].dtype == object:
            out["value"] = (
                out["value"].str.replace(",", "").str.replace("N/A", "0").astype(int)
            )

        cols_to_keep = [
            "vintage",
            "variable",
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



class CDCVaccinePfizer(CDCVaccineBase):
    query_type = "pfizer"
    source = "https://data.cdc.gov/Vaccinations/COVID-19-Vaccine-Distribution-Allocations-by-Juris/saz5-9hgg"
    crename = {
        "total_pfizer_allocation_first_dose_shipments": CMU(
            category="pfizer_vaccine_first_dose_allocated",
            measurement="cumulative",
            unit="doses",
        ),
        "total_allocation_pfizer_second_dose_shipments": CMU(
            category="pfizer_vaccine_second_dose_allocated",
            measurement="cumulative",
            unit="doses",
        ),
        "doses_allocated_week_of": CMU(
            category="pfizer_vaccine_first_dose_allocated",
            measurement="new_7_day",
            unit="doses",
        ),
        "second_dose_shipment_week_of": CMU(
            category="pfizer_vaccine_second_dose_allocated",
            measurement="new_7_day",
            unit="doses",
        ),
    }


class CDCVaccineModerna(CDCVaccineBase):
    query_type = "moderna"
    source = "https://data.cdc.gov/Vaccinations/COVID-19-Vaccine-Distribution-Allocations-by-Juris/b7pe-5nws"
    crename = {
        "total_moderna_allocation_first_dose_shipments": CMU(
            category="moderna_vaccine_first_dose_allocated",
            measurement="cumulative",
            unit="doses",
        ),
        "total_allocation_moderna_second_dose_shipments": CMU(
            category="moderna_vaccine_second_dose_allocated",
            measurement="cumulative",
            unit="doses",
        ),
    }
