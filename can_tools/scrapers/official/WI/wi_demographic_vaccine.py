import pandas as pd
import us
from can_tools.scrapers.official.base import TableauDashboard
from can_tools.scrapers.official.WI.wi_county_vaccine import WisconsinVaccineCounty
from can_tools.scrapers.util import requests_retry_session
from bs4 import BeautifulSoup
import json
from urllib.parse import parse_qs, urlparse
import urllib.parse
import re
import requests
from can_tools.scrapers import variables, CMU

class WisconsinVaccineAge(WisconsinVaccineCounty):
    data_tableau_table = "Age vax/unvax County"
    # age does not report missing/unknown entries
    missing_tableau_table = ""
    location_name_col = "AGG(Geography TT)-alias"
    location_type = "state"

    # map wide form column names into CMUs
    cmus = {
        "SUM(Initiation or completed count for TT)-alias": CMU(
            category="total_vaccine_initiated",
            measurement="cumulative",
            unit="people",
        )
    }


    def _get_demographic(
        self, df: pd.DataFrame, demo: str, demo_col_name: str
    ) -> pd.DataFrame:
        """
        description: a general "normalize" function to avoid extra/copied code
                     each demographic uses this in its respective normalize

        params:
            demo: the demographic as labeled according to CMU (age,sex,race, etc...)
            demo_col_name: the name of the demographic column from the fetched data

        returns: normalized data in long format
        """

        # county names (converted to title case)
        df["location_name"] = df[self.location_name_col].str.title()
        # fix county names
        df = df.replace(
            {"location_name": {"St Croix": "St. Croix", "Fond Du Lac": "Fond du Lac"}}
        )

        # parse out data columns
        value_cols = list(set(df.columns) & set(self.cmus.keys()))
        assert len(value_cols) == len(self.cmus)

        df = (
            df.melt(id_vars=[demo_col_name, "location_name"], value_vars=value_cols)
            .dropna()
            .assign(
                dt=self._retrieve_dt(self.timezone),
                vintage=self._retrieve_vintage(),
                value=lambda x: pd.to_numeric(
                    x["value"].astype(str).str.replace(",", "")
                ),
            )
            .pipe(self.extract_CMU, cmu=self.cmus)
        )
        df[demo] = df[demo_col_name]
        return df.drop(["variable", demo_col_name], axis=1)

    def fetch(self) -> pd.DataFrame:
        if self.missing_tableau_table:
            # extract both data table and missing data table
            dfs = [
                self.get_tableau_view().get(table)
                for table in [self.data_tableau_table, self.missing_tableau_table]
            ]
            return pd.concat(dfs)
        else:
            return self.get_tableau_view()[self.data_tableau_table]

    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        df = self._get_demographic(df, "age", "Age-value")
        return df.replace({"age": {"65+": "65_plus"}})


class WisconsinVaccineRace(WisconsinVaccineAge):
    data_tableau_table = "Race vax/unvax county"
    missing_tableau_table = "Race missing county"

    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        df = self._get_demographic(df, "race", "Race-value")
        rm = {
            "1": "white",
            "2": "black",
            "3": "native_american",
            "4": "asian",
            "5": "other",
            "U": "unknown",
        }
        return df.replace({"race": rm})


class WisconsinVaccineSex(WisconsinVaccineAge):
    data_tableau_table = "Sex vax/unvax county"
    missing_tableau_table = "Sex missing county"

    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        df = self._get_demographic(df, "sex", "Sex-value")
        df["sex"] = df["sex"].str.lower()
        return df


class WisconsinVaccineEthnicity(WisconsinVaccineAge):
    data_tableau_table = "Ethnicity vax/unvax county"
    missing_tableau_table = "Ethnicity missing county"

    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        df = self._get_demographic(df, "ethnicity", "Ethnicity-value")
        df["ethnicity"] = df["ethnicity"].str.lower()
        return df

class WisconsinVaccineCountyDemographics(WisconsinVaccineCounty):

    variables = {
        "total_vaccine_initiated": variables.INITIATING_VACCINATIONS_ALL,
    }

    def fetch(self):
        """
        make initial request (to open connection + get session id)
        """
        req = requests_retry_session()
        fullURL = self.baseurl + "/views/" + self.viewPath
        reqg = req.get(
                fullURL,
                params={
                    ":language": "en",
                    ":display_count": "y",
                    ":origin": "viz_share_link",
                    ":embed": "y",
                    ":showVizHome": "n",
                    ":jsdebug": "y",
                    ":apiID": "host4",
                    "#navType": "1",
                    "navSrc": "Parse",
                },
                headers={"Accept": "text/javascript"},
        )
        soup = BeautifulSoup(reqg.text, "html.parser")
        tableauTag = soup.find("textarea", {"id": "tsConfigContainer"})
        tableauData = json.loads(tableauTag.text)
        parsed_url = urllib.parse.urlparse(fullURL)
        dataUrl = f'{parsed_url.scheme}://{parsed_url.hostname}{tableauData["vizql_root"]}/bootstrapSession/sessions/{tableauData["sessionid"]}'
        r = requests.post(dataUrl, data= {
            "sheet_id": tableauData["sheetId"],
        })

        """
        find county locations on dashboard + their index
        the order of the counties is important, as this is the order that the data is stored/returned 
        (see loops below)
        """  
        dataReg = re.search('\d+;({.*})\d+;({.*})', r.text, re.MULTILINE)
        info = json.loads(dataReg.group(1))
        data = json.loads(dataReg.group(2))

        # "Map" and "[Calculation_328762828852510720]" are dashboard specific
        # document how to find these
        stateIndexInfo = [ 
            (t["fieldRole"], {
                "paneIndices": t["paneIndices"][0], 
                "columnIndices": t["columnIndices"][0], 
                "dataType": t["dataType"]
            }) 
            for t in data["secondaryInfo"]["presModelMap"]["vizData"]["presModelHolder"]["genPresModelMapPresModel"]["presModelMap"]["Map"]["presModelHolder"]["genVizDataPresModel"]["paneColumnsData"]["vizDataColumns"]
            if t.get("localBaseColumnName") and t["localBaseColumnName"] == "[Calculation_328762828852510720]"
        ]
        stateNameIndexInfo = [t[1] for t in stateIndexInfo if t[0] == 'dimension'][0]
        panelColumnList = data["secondaryInfo"]["presModelMap"]["vizData"]["presModelHolder"]["genPresModelMapPresModel"]["presModelMap"]["Map"]["presModelHolder"]["genVizDataPresModel"]["paneColumnsData"]["paneColumnsList"]
        stateNameIndices = panelColumnList[stateNameIndexInfo["paneIndices"]]["vizPaneColumns"][stateNameIndexInfo["columnIndices"]]["valueIndices"]
        dataValues = [
            t for t in data["secondaryInfo"]["presModelMap"]["dataDictionary"]["presModelHolder"]["genDataDictionaryPresModel"]["dataSegments"]["0"]["dataColumns"]
            if t["dataType"] == stateNameIndexInfo["dataType"]
        ][0]["dataValues"]
        countyNames = [dataValues[t] for t in stateNameIndices]

        """
        simulate "selection" request for each county -- extract data and store in list.

        If it's possible to return multiple counties in one call, we should do that to be more efficient.
        But, this works
        """
        county_data = []
        for i, name in enumerate(countyNames):
            if i == 3:
                break
            r = requests.post(f'https://bi.wisconsin.gov{tableauData["vizql_root"]}/sessions/{tableauData["sessionid"]}/commands/tabdoc/select',
                data = {
                "worksheet": "Map",
                "dashboard": "VaccinatedWisconsin-County",
                "selection": json.dumps({
                    "objectIds":[i+1],
                    "selectionType":"tuples"
                }),
                "selectOptions": "select-options-simple"
            })
            #isolate covid data
            dataSegments = r.json()["vqlCmdResponse"]["layoutStatus"]["applicationPresModel"]["dataDictionary"]["dataSegments"]
            d = dataSegments[list(dataSegments.keys())[0]]["dataColumns"][0]['dataValues']
            # add name to start of list
            d.insert(0, name)
            county_data.append(list(d))
        
        return county_data

    
    def normalize(self, data):
        age = self._handle_demographic(data, [1,8], 'total_vaccine_initiated', 'age')
        return age

    def _handle_demographic(self, data, idx:list, category:str, demographic:str):
        data = [d[0:1] + d[idx[0]:idx[1]] for d in data]
        df = pd.DataFrame(data, columns = ['location_name', '65+', '55-64', '45-54','35-44','25-34','18-24','16-17'])
        df = df.melt(id_vars=["location_name"], var_name='demo_val')
        df['variable'] = category
        df = self.extract_CMU(df, self.variables).drop(columns={'age'}).rename(columns={'demo_val':demographic})
        return df