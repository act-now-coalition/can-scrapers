import pandas as pd
import us
import lxml.html
import requests
from abc import abstractmethod
from can_tools.scrapers.official.base import StateDashboard

class DCBase(StateDashboard):
    has_location = False
    state_fips = us.states.lookup("DC").fips
    location_type = "state"
    source = "https://coronavirus.dc.gov/page/coronavirus-data"

    def fetch(self) -> pd.ExcelFile:
        """
        locate most recent data export, download excel file

        Returns
        -------
        df: pd.ExcelFile
            A pandas Excel File
        """
        res = requests.get(self.source)
        if not res.ok:
            raise ValueError("Could not fetch source of DC page")
        tree = lxml.html.fromstring(res.content)
        links = tree.xpath('//a[contains(text(), "Download copy")]/@href') #find all download links

        #selects most recent (yesterdays) file by taking the first off the stack
        if links[0].startswith('https'):
            xl_src = links[0]
        elif links[0].startswith('/'):
            xl_src = 'https://coronavirus.dc.gov' + links[0]
        else: raise ValueError("Could not parse download link")

        #download xlsx from selected link
        xl = requests.get(xl_src) 
        if not xl.ok:
            raise ValueError("Could not fetch download file")

        return pd.ExcelFile(xl.content) #read file into pd excel object
    
    def _reshape(self, data: pd.DataFrame, map: dict) -> pd.DataFrame:
        """
        Function to prep data for put() function. renames and adds columns according to CMU (map) entries
            
            Accepts
            ------- 
                data: df w/ column names according the map parameter 
                    example of format: 
                                dt   Variable Name  ...         location_name
                    0   2020-03-07  Variable Value  ...  District of Columbia
                    ...
                map: dictionary with CMU keys/values
            
            Returns
            ------- 
                pd.Dataframe: dataframe ready for put() function     
        """

        out = data.melt(
            id_vars=["dt","location_name"], value_vars=map.keys()
        ).dropna()
        out.loc[:, "value"] = pd.to_numeric(out["value"])

        out = self.extract_CMU(out, map)
        out["vintage"] = self._retrieve_vintage()

        cols_to_keep = [
            "vintage",
            "dt",
            "location_name",
            "category",
            "measurement",
            "unit",
            "age",
            "race",
            "sex",
            "value",
        ]

        return out.loc[:, cols_to_keep] 

    @abstractmethod
    def _wrangle(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Parent function to re-structure df into standard form. 
        Used in child class to transpose and clean data returned from DC's excel data exports 
            Accepts
            ------- 
                data: pd.Dataframe 
            Returns
            ------- 
                pd.DataFrame    
        """




