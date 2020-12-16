import requests
import re
from bs4 import BeautifulSoup
import json
import pandas as pd

from can_tools.scrapers.base import CMU
from can_tools.scrapers.official.base import StateQueryAPI

class California(StateQueryAPI):

    base_url = "https://public.tableau.com"

    def fetch(self):
       return 

    def normalize(self):
        testing = self._get_testing()
        # cases = self._get_cases()

        # df = pd.concat([testing, cases], axis=0).sort_values(["dt", "county"])
        df = pd.concat([testing],axis=0).sort_values(["dt"])
        df["vintage"] = self._retrieve_vintage()

        return df

    def _get_testing(self):
        viewPath = "StateDashboard_16008816705240/6_1CountyTesting"
        data = self._scrape_view(viewPath)
        df = data['6.3 County Test - Line (2)']
        df["dt"] = pd.to_datetime(df["DAY(Test Date)-value"])
        # df["measurement"] = "pcr_tests_total"
        # df = df.rename(
        #     columns={
        #     "value": "variable"
        # })
        crename = {
            "SUM(Tests)-value": CMU(
                category="pcr_tests_total",
                measurement="new",
                unit="specimens",
            ),
        }
        # renamed["county"] = renamed["county"].apply(lambda x: x.lower().capitalize())
        df = df.query("dt != '1970-01-01'").melt(id_vars=["dt"],value_vars=crename.keys()).dropna()
        df = self.extract_CMU(df, crename)

        cols_to_keep = [
            "dt",
            "category",
            "measurement",
            "unit",
            "age",
            "race",
            "sex",
            "value",
        ]

        return df.loc[:, cols_to_keep]

    def _scrape_view(self, viewPath):
        def onAlias(it, value, cstring):
            return value[it] if (it >= 0) else cstring["dataValues"][abs(it) - 1]

        req = requests.Session()
        fullURL = self.base_url + "/views/" + viewPath
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
        )
        soup = BeautifulSoup(reqg.text, "html.parser")
        tableauTag = soup.find("textarea", {"id": "tsConfigContainer"})
        tableauData = json.loads(tableauTag.text)
        dataUrl = f'{self.base_url}/{tableauData["vizql_root"]}/bootstrapSession/sessions/{tableauData["sessionid"]}'

        resp = requests.post(
            dataUrl,
            data={
                "sheet_id": tableauData["sheetId"],
            },
        )
        # Parse the response.
        # The response contains multiple chuncks of the form
        # `<size>;<json>` where `<size>` is the number of bytes in `<json>`
        resp_text = resp.text
        data = []
        while len(resp_text) != 0:
            size, rest = resp_text.split(";", 1)
            chunck = json.loads(rest[: int(size)])
            data.append(chunck)
            resp_text = rest[int(size) :]

        # The following section (to the end of the method) uses code from
        # https://stackoverflow.com/questions/64094560/how-do-i-scrape-tableau-data-from-website-into-r
        presModel = data[1]["secondaryInfo"]["presModelMap"]
        metricInfo = presModel["vizData"]["presModelHolder"]
        metricInfo = metricInfo["genPresModelMapPresModel"]["presModelMap"]
        data = presModel["dataDictionary"]["presModelHolder"]
        data = data["genDataDictionaryPresModel"]["dataSegments"]["0"]["dataColumns"]

        scrapedData = {}

        for metric in metricInfo:
            metricsDict = metricInfo[metric]["presModelHolder"]["genVizDataPresModel"]
            columnsData = metricsDict["paneColumnsData"]

            result = [
                {
                    "fieldCaption": t.get("fieldCaption", ""),
                    "valueIndices": columnsData["paneColumnsList"][t["paneIndices"][0]][
                        "vizPaneColumns"
                    ][t["columnIndices"][0]]["valueIndices"],
                    "aliasIndices": columnsData["paneColumnsList"][t["paneIndices"][0]][
                        "vizPaneColumns"
                    ][t["columnIndices"][0]]["aliasIndices"],
                    "dataType": t.get("dataType"),
                    "paneIndices": t["paneIndices"][0],
                    "columnIndices": t["columnIndices"][0],
                }
                for t in columnsData["vizDataColumns"]
                if t.get("fieldCaption")
            ]
            frameData = {}
            cstring = [t for t in data if t["dataType"] == "cstring"][0]
            for t in data:
                for index in result:
                    if t["dataType"] == index["dataType"]:
                        if len(index["valueIndices"]) > 0:
                            frameData[f'{index["fieldCaption"]}-value'] = [
                                t["dataValues"][abs(it)] for it in index["valueIndices"]
                            ]
                        if len(index["aliasIndices"]) > 0:
                            frameData[f'{index["fieldCaption"]}-alias'] = [
                                onAlias(it, t["dataValues"], cstring)
                                for it in index["aliasIndices"]
                            ]

            df = pd.DataFrame.from_dict(frameData, orient="index").fillna(0).T

            scrapedData[metric] = df

        return scrapedData
