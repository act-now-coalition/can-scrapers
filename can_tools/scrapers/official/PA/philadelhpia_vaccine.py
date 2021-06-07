import json
import pandas as pd
from us import states
from bs4 import BeautifulSoup
import urllib.parse
import json

from can_tools.scrapers import variables
from can_tools.scrapers.official.base import TableauDashboard
from can_tools.scrapers.util import requests_retry_session


class PhiladelphiaVaccine(TableauDashboard):
    state_fips = int(states.lookup("Pennsylvania").fips)
    has_location = True
    location_type = "county"
    source = (
        "https://www.phila.gov/programs/coronavirus-disease-2019-covid-19/data/vaccine/"
    )
    source_name = "Philadelphia Department of Public Health"
    baseurl = "https://healthviz.phila.gov/t/PublicHealth/"
    viewPath = "COVIDVaccineDashboard/COVID_Vaccine"
    data_tableau_table = "Residents Percentage New"
    variables = {
        "Residents Receiving At Least 1 Dose ": variables.INITIATING_VACCINATIONS_ALL,
        "Full Vaccinated Residents": variables.FULLY_VACCINATED_ALL,
    }

    def normalize(self, data: pd.DataFrame) -> pd.DataFrame:
        return (
            data.rename(
                columns={
                    "Measure Values-alias": "value",
                    "Measure Names-alias": "variable",
                }
            )
            .loc[:, ["value", "variable"]]
            .query(
                "variable in"
                "['Residents Receiving At Least 1 Dose ', 'Full Vaccinated Residents']"
            )
            .assign(
                location=42101,
                value=lambda x: pd.to_numeric(x["value"].str.replace(",", "")),
                vintage=self._retrieve_vintage(),
            )
            .pipe(
                self._rename_or_add_date_and_location,
                location_column="location",
                timezone="US/Eastern",
            )
            .pipe(self.extract_CMU, cmu=self.variables)
            .drop(columns={"variable"})
            .reset_index(drop=True)
        )

    # could not find a way to select the "Demographics New" dashboard tab in the usual manner,
    # so edit request body to manually select Demographic tab/sheets
    # this is the default function with only form_data["sheet_id"] altered
    def get_tableau_view(self, url=None):
        def onAlias(it, value, cstring):
            return value[it] if (it >= 0) else cstring["dataValues"][abs(it) - 1]

        req = requests_retry_session()
        fullURL = self.baseurl + "/views/" + self.viewPath
        if url is not None:
            fullURL = url
        if self.filterFunctionName is not None:
            params = ":language=en&:display_count=y&:origin=viz_share_link&:embed=y&:showVizHome=n&:jsdebug=y&"
            params += self.filterFunctionName + "=" + self.filterFunctionValue
            if self.secondaryFilterFunctionName is not None:
                params += (
                    "&"
                    + self.secondaryFilterFunctionName
                    + "="
                    + self.secondaryFilterValue.replace(" ", "%20")
                )
            reqg = req.get(fullURL, params=params)
        else:
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

        # copy over some additional headers from tableauData
        form_data = {}
        form_map = {
            "sheetId": "sheet_id",
            "showParams": "showParams",
            "stickySessionKey": "stickySessionKey",
        }
        for k, v in form_map.items():
            if k in tableauData:
                form_data[v] = tableauData[k]

        # set sheet manually to access the subsheets we need
        form_data["sheet_id"] = "Demographics New"
        resp = req.post(
            dataUrl,
            data=form_data,
            headers={"Accept": "text/javascript"},
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
