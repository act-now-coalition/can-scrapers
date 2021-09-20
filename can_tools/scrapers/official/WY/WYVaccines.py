import enum
import json
from typing import Any

import pandas as pd
import requests
import re
import us
import jmespath
from bs4 import BeautifulSoup as bs

from can_tools.scrapers.base import CMU
from can_tools.scrapers import variables
from can_tools.scrapers.official.base import GoogleDataStudioDashboard, StateDashboard


class WYStateVaccinations(StateDashboard):
    state_fips = int(us.states.lookup("Wyoming").fips)
    execution_dt = pd.Timestamp.now()
    source = (
        "https://health.wyo.gov/publichealth/immunization/wyoming-covid-19-vaccine-information/covid-19-"
        "vaccine-distribution-data/"
    )
    source_name = "Wyoming Department of Health"
    has_location = True
    location_type = "state"

    variables = {
        "total_doses_administered": variables.TOTAL_DOSES_ADMINISTERED_ALL,
        "total_vaccine_initiated": variables.INITIATING_VACCINATIONS_ALL,
        "total_vaccine_completed": variables.FULLY_VACCINATED_ALL,
    }

    def fetch(self):
        header = {
            "Referer": "https://google.com/",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
        }
        return requests.get(self.source, headers=header).text

    def _extract_data(self, match, data):
        # use regex to find the tags that follow the name the doses (eg: "Total Doses: <strong>...data...</strong>")
        end = re.findall(f"{match}:\s?<strong>(.*?)</strong>", data)[0]

        # convert back to an HTML object to extract the text of the tag containing the actual data
        end = bs(end, "html.parser").find_all("span")[0].text
        return pd.to_numeric(end.replace(",", ""))

    def normalize(self, data) -> pd.DataFrame:
        # find data in page
        soup = bs(data, "lxml")
        data = soup.select(
            "div.et_pb_module.et_pb_text.et_pb_text_0.et_pb_text_align_left.et_pb_bg_layout_light"
        )[0]
        data = data.find_all("p")
        data = [str(p) for p in data]

        # extract data from strings with regex -- assert that we collect correct values
        assert bool(re.search("Overall Totals", data[1]))
        doses_admin = self._extract_data("Total Doses Administered", data[1])

        assert bool(re.search("Two-Dose Vaccines", data[2]))
        pfiz_mod_1_dose = self._extract_data("First Doses Administered", data[2])
        pfiz_mod_2_dose = self._extract_data("Second Doses Administered", data[2])

        assert bool(re.search("One-Dose Vaccine", data[3]))
        jj_doses = self._extract_data("Doses Administered", data[3])

        records = [
            {"variable": "total_doses_administered", "value": doses_admin},
            {
                "variable": "total_vaccine_initiated",
                "value": pfiz_mod_1_dose + jj_doses,
            },
            {
                "variable": "total_vaccine_completed",
                "value": pfiz_mod_2_dose + jj_doses,
            },
        ]

        return (
            pd.DataFrame.from_records(records)
            .assign(
                dt=self._retrieve_dtm1d(),
                vintage=self._retrieve_vintage(),
                location=56,
            )
            .pipe(self.extract_CMU, cmu=self.variables)
            .drop(columns={"variable"})
        )


class WYCountyVaccinations(GoogleDataStudioDashboard):
    """
    Pulls Wyoming state vaccine data, cleans and up the Json Strig that is returned before returning that object as
    a dictionary
    """

    state_fips = int(us.states.lookup("Wyoming").fips)
    source = (
        "https://health.wyo.gov/publichealth/immunization/wyoming-covid-19-vaccine-information/"
        "covid-19-vaccine-distribution-data/"
    )
    source_name = "Wyoming Department of Health"
    baseUrl = "https://datastudio.google.com/batchedDataV2"
    has_location = False
    location_type = "county"

    variables = {
        "initiated": variables.INITIATING_VACCINATIONS_ALL,
        "completed": variables.FULLY_VACCINATED_ALL,
    }

    key_names = {
        "qt_aa5c4yu7ic": "location_name",
        "qt_pweig0u7ic": "one_dose_only",
        "qt_rrtnz0u7ic": "two_dose_only",
        "qt_oh3q30u7ic": "jj_doses",
    }

    def fetch(self):
        """
        Pulls Wyoming county vaccine data, cleans and up the Json Strig that is returned before returning that object as
        a dictionary
        """
        request_body = {
            "dataRequest": [
                {
                    "datasetSpec": {
                        "contextNsCount": 1,
                        "dataset": [
                            {
                                "datasourceId": "f264378a-4e61-41cf-9017-ba25e1a3ba22",
                            }
                        ],
                        "filters": [
                            {
                                "dataSubsetNs": {
                                    "contextNs": "c0",
                                    "datasetNs": "d0",
                                    "tableNs": "t0",
                                },
                                "filterDefinition": {
                                    "filterExpression": {
                                        "concept": {
                                            "name": "qt_cn2odwj1fc",
                                            "ns": "t0",
                                        },
                                        "conceptType": 0,
                                        "filterConditionType": "EQ",
                                        "include": False,
                                        "numberValues": [],
                                        "queryTimeTransformation": {
                                            "dataTransformation": {
                                                "sourceFieldName": "_2024258922_"
                                            }
                                        },
                                        "stringValues": ["Wyoming"],
                                    }
                                },
                                "version": 3,
                            }
                        ],
                        "queryFields": [
                            {
                                "datasetNs": "d0",
                                "dataTransformation": {
                                    "sourceFieldName": "_2024258922_"
                                },
                                "name": "qt_aa5c4yu7ic",
                                "tableNs": "t0",
                            },
                            {
                                "datasetNs": "d0",
                                "dataTransformation": {
                                    "aggregation": 6,
                                    "sourceFieldName": "_n274042269_",
                                },
                                "name": "qt_pweig0u7ic",
                                "tableNs": "t0",
                            },
                            {
                                "datasetNs": "d0",
                                "dataTransformation": {
                                    "aggregation": 6,
                                    "sourceFieldName": "_n274042238_",
                                },
                                "name": "qt_rrtnz0u7ic",
                                "tableNs": "t0",
                            },
                            {
                                "datasetNs": "d0",
                                "dataTransformation": {
                                    "aggregation": 6,
                                    "sourceFieldName": "_n1110901404_",
                                },
                                "name": "qt_oh3q30u7ic",
                                "tableNs": "t0",
                            },
                        ],
                    },
                    "requestContext": {
                        "reportContext": {
                            "componentId": "cd-994c4yu7ic",
                            "pageId": "26374700",
                            "reportId": "59351f95-49e8-4440-b752-4d9b919dca88",
                        }
                    },
                    "useDataColumn": True,
                },
            ]
        }

        response = self.get_dataset(request_body, url=self.baseUrl)
        return response

    def normalize(self, data) -> pd.DataFrame:
        # extract json from response
        search = r"\{\"dataResponse\":\[.*\}\]\}"
        json_body = json.loads(re.findall(search, data, flags=re.MULTILINE)[0])
        rawdata = json_body["dataResponse"][0]["dataSubset"][0]["dataset"]
        columns = rawdata["tableDataset"]["column"]

        # get the order of the variables from request, map them to readable values
        variable_names = rawdata["tableDataset"]["columnInfo"]
        variable_names = [v["name"] for v in variable_names]
        variable_names = [self.key_names.get(item, item) for item in variable_names]

        # variable_names and columns have same order, build df from both
        df = pd.DataFrame()
        for name, data in zip(variable_names, columns):
            if "stringColumn" in data.keys():
                df[name] = data["stringColumn"]["values"]
            else:
                df[name] = data["doubleColumn"]["values"]

        # create variables to match our def'ns
        out = df.assign(
            initiated=lambda x: x["one_dose_only"] + x["jj_doses"],
            completed=lambda x: x["two_dose_only"] + x["jj_doses"],
            dt=self._retrieve_dtm1d("US/Mountain"),
            vintage=self._retrieve_vintage(),
        )
        return self._reshape_variables(out, variable_map=self.variables)


class WYCountyAgeVaccinations(GoogleDataStudioDashboard):
    """
    Pulls Wyoming state vaccine data, cleans and up the Json Strig that is returned before returning that object as
    a dictionary
    """

    # JSONs to pass to API to get county level vaccine dosage informtation
    bodyTotalPercentage = '{"dataRequest":[{"requestContext":{"reportContext":{"reportId":"59351f95-49e8-4440-b752-4d9b919dca88","pageId":"26374700","mode":"VIEW","componentId":"cd-rh77r0tlic","displayType":"simple-barchart"}},"datasetSpec":{"dataset":[{"datasourceId":"f264378a-4e61-41cf-9017-ba25e1a3ba22","revisionNumber":0,"parameterOverrides":[]}],"queryFields":[{"name":"qt_4mbkocs6ic","datasetNs":"d0","tableNs":"t0","dataTransformation":{"sourceFieldName":"_2024258922_"}},{"name":"qt_v2ebxcs6ic","datasetNs":"d0","tableNs":"t0","dataTransformation":{"sourceFieldName":"_n765467659_","aggregation":6}}],"sortData":[{"sortColumn":{"name":"qt_4mbkocs6ic","datasetNs":"d0","tableNs":"t0","dataTransformation":{"sourceFieldName":"_2024258922_"}},"sortDir":0}],"includeRowsCount":true,"paginateInfo":{"startRow":1,"rowsCount":25},"blendConfig":{"blockDatasource":{"datasourceBlock":{"id":"block_yom17tjijc","type":1,"inputBlockIds":[],"outputBlockIds":[],"fields":[]},"blocks":[{"id":"block_zom17tjijc","type":5,"inputBlockIds":[],"outputBlockIds":[],"fields":[],"queryBlockConfig":{"joinQueryConfig":{"joinKeys":[],"queries":[{"datasourceId":"f264378a-4e61-41cf-9017-ba25e1a3ba22","concepts":[]}]}}}],"delegatedAccessEnabled":true,"isUnlocked":true,"isCacheable":false}},"filters":[{"filterDefinition":{"filterExpression":{"include":false,"conceptType":0,"concept":{"ns":"t0","name":"qt_cn2odwj1fc"},"filterConditionType":"EQ","stringValues":["Wyoming"],"numberValues":[],"queryTimeTransformation":{"dataTransformation":{"sourceFieldName":"_2024258922_"}}}},"dataSubsetNs":{"datasetNs":"d0","tableNs":"t0","contextNs":"c0"},"version":3}],"features":[],"dateRanges":[],"contextNsCount":1,"calculatedField":[],"needGeocoding":false,"geoFieldMask":[],"geoVertices":100000},"useDataColumn":true}]}'
    body18UpPercentage = '{"dataRequest":[{"requestContext":{"reportContext":{"reportId":"59351f95-49e8-4440-b752-4d9b919dca88","pageId":"26374700","mode":"VIEW","componentId":"cd-vtpds4tlic","displayType":"simple-barchart"}},"datasetSpec":{"dataset":[{"datasourceId":"f264378a-4e61-41cf-9017-ba25e1a3ba22","revisionNumber":0,"parameterOverrides":[]}],"queryFields":[{"name":"qt_h5hb7ds6ic","datasetNs":"d0","tableNs":"t0","dataTransformation":{"sourceFieldName":"_2024258922_"}},{"name":"qt_g8sr9ds6ic","datasetNs":"d0","tableNs":"t0","dataTransformation":{"sourceFieldName":"_1723449677_","aggregation":6}}],"sortData":[{"sortColumn":{"name":"qt_h5hb7ds6ic","datasetNs":"d0","tableNs":"t0","dataTransformation":{"sourceFieldName":"_2024258922_"}},"sortDir":0}],"includeRowsCount":true,"paginateInfo":{"startRow":1,"rowsCount":25},"blendConfig":{"blockDatasource":{"datasourceBlock":{"id":"block_n0o17tjijc","type":1,"inputBlockIds":[],"outputBlockIds":[],"fields":[]},"blocks":[{"id":"block_o0o17tjijc","type":5,"inputBlockIds":[],"outputBlockIds":[],"fields":[],"queryBlockConfig":{"joinQueryConfig":{"joinKeys":[],"queries":[{"datasourceId":"f264378a-4e61-41cf-9017-ba25e1a3ba22","concepts":[]}]}}}],"delegatedAccessEnabled":true,"isUnlocked":true,"isCacheable":false}},"filters":[{"filterDefinition":{"filterExpression":{"include":false,"conceptType":0,"concept":{"ns":"t0","name":"qt_cn2odwj1fc"},"filterConditionType":"EQ","stringValues":["Wyoming"],"numberValues":[],"queryTimeTransformation":{"dataTransformation":{"sourceFieldName":"_2024258922_"}}}},"dataSubsetNs":{"datasetNs":"d0","tableNs":"t0","contextNs":"c0"},"version":3}],"features":[],"dateRanges":[],"contextNsCount":1,"calculatedField":[],"needGeocoding":false,"geoFieldMask":[],"geoVertices":100000},"useDataColumn":true}]}'
    body65UpPercentage = '{"dataRequest":[{"requestContext":{"reportContext":{"reportId":"59351f95-49e8-4440-b752-4d9b919dca88","pageId":"26374700","mode":"VIEW","componentId":"cd-tarmc5tlic","displayType":"simple-barchart"}},"datasetSpec":{"dataset":[{"datasourceId":"f264378a-4e61-41cf-9017-ba25e1a3ba22","revisionNumber":0,"parameterOverrides":[]}],"queryFields":[{"name":"qt_6dfwfes6ic","datasetNs":"d0","tableNs":"t0","dataTransformation":{"sourceFieldName":"_2024258922_"}},{"name":"qt_lhchles6ic","datasetNs":"d0","tableNs":"t0","dataTransformation":{"sourceFieldName":"_1863824869_","aggregation":6}}],"sortData":[{"sortColumn":{"name":"qt_6dfwfes6ic","datasetNs":"d0","tableNs":"t0","dataTransformation":{"sourceFieldName":"_2024258922_"}},"sortDir":0}],"includeRowsCount":true,"paginateInfo":{"startRow":1,"rowsCount":25},"blendConfig":{"blockDatasource":{"datasourceBlock":{"id":"block_ssp17tjijc","type":1,"inputBlockIds":[],"outputBlockIds":[],"fields":[]},"blocks":[{"id":"block_tsp17tjijc","type":5,"inputBlockIds":[],"outputBlockIds":[],"fields":[],"queryBlockConfig":{"joinQueryConfig":{"joinKeys":[],"queries":[{"datasourceId":"f264378a-4e61-41cf-9017-ba25e1a3ba22","concepts":[]}]}}}],"delegatedAccessEnabled":true,"isUnlocked":true,"isCacheable":false}},"filters":[{"filterDefinition":{"filterExpression":{"include":false,"conceptType":0,"concept":{"ns":"t0","name":"qt_cn2odwj1fc"},"filterConditionType":"EQ","stringValues":["Wyoming"],"numberValues":[],"queryTimeTransformation":{"dataTransformation":{"sourceFieldName":"_2024258922_"}}}},"dataSubsetNs":{"datasetNs":"d0","tableNs":"t0","contextNs":"c0"},"version":3}],"features":[],"dateRanges":[],"contextNsCount":1,"calculatedField":[],"needGeocoding":false,"geoFieldMask":[],"geoVertices":100000},"useDataColumn":true}]}'

    state_fips = int(us.states.lookup("Wyoming").fips)
    execution_dt = pd.Timestamp.now()
    source = (
        "https://health.wyo.gov/publichealth/immunization/wyoming-covid-19-vaccine-information/"
        "covid-19-vaccine-distribution-data/"
    )
    source_name = "Wyoming Department of Health"
    baseUrl = "https://datastudio.google.com/batchedDataV2"
    resource_id = "a51cf808-9bf3-44a0-bd26-4337aa9f8700"
    has_location = False
    location_type = "county"

    def fetch(self):
        """
        Pulls Wyoming county vaccine data, cleans and up the Json Strig that is returned before returning that object as
        a dictionary
        """
        WYVacDataTotal = self.get_dataset(
            json.loads(self.bodyTotalPercentage), url=self.baseUrl
        )
        parsedVacDataTotal = json.loads(WYVacDataTotal[11 : len(WYVacDataTotal) - 1])
        WYVacData18P = self.get_dataset(
            json.loads(self.body18UpPercentage), url=self.baseUrl
        )
        parsedVacData18P = json.loads(WYVacData18P[11 : len(WYVacData18P) - 1])
        WYVacData65P = self.get_dataset(
            json.loads(self.body65UpPercentage), url=self.baseUrl
        )
        parsedVacData65P = json.loads(WYVacData65P[11 : len(WYVacData65P) - 1])

        query = jmespath.compile(
            "dataResponse[0].dataSubset[0].dataset.tableDataset.column"
        )

        return (
            query.search(parsedVacDataTotal)
            + query.search(parsedVacData18P)
            + query.search(parsedVacData65P)
        )

    def normalize(self, data) -> pd.DataFrame:
        countiesTotal = data[0]["stringColumn"]["values"]
        countiesTotalNullIndex = data[0]["nullIndex"]
        countiesTotalPer = data[1]["doubleColumn"]["values"]
        countiesTotalPerNullIndex = data[1]["nullIndex"]

        # sorting null index lists so there is no chance of an index out of bounds error when we insert 0 values for
        # null indexes
        countiesTotalNullIndex.sort()
        countiesTotalPerNullIndex.sort()

        # inserting values of 0 for the indexes corresponding with the null value indexes
        for value in countiesTotalNullIndex:
            countiesTotal.insert(value, 0)
        for value in countiesTotalPerNullIndex:
            countiesTotalPer.insert(value, 0)

        # create dose 1 data frame
        countyVaccineDataFrameAll = pd.DataFrame(
            {
                "location_name": countiesTotal,
                "TotalPer": countiesTotalPer,
            }
        )
        countyVaccineDataFrameAll["TotalPer"] = (
            countyVaccineDataFrameAll["TotalPer"] * 100
        )
        countyVaccineDataFrameAll["age"] = "all"
        counties18plus = data[2]["stringColumn"]["values"]
        counties18PlusNullIndex = data[2]["nullIndex"]
        counties18PlusPer = data[3]["doubleColumn"]["values"]
        counties18PlusPerNullIndex = data[3]["nullIndex"]

        # sorting null index lists so there is no chance of an index out of bounds error when we insert 0 values
        counties18PlusNullIndex.sort()
        counties18PlusPerNullIndex.sort()

        # inserting values of 0 for the indexes corresponding with the null value indexes
        for value in counties18PlusNullIndex:
            counties18plus.insert(value, 0)
        for value in counties18PlusPerNullIndex:
            counties18PlusPer.insert(value, 0)

        # create dose 2 dataframe
        countyVaccineDataFrame18 = pd.DataFrame(
            {
                "location_name": counties18plus,
                "TotalPer": counties18PlusPer,
            }
        )
        countyVaccineDataFrame18["age"] = "18_plus"
        countyVaccineDataFrame18["TotalPer"] = (
            countyVaccineDataFrame18["TotalPer"] * 100
        )
        counties65plus = data[4]["stringColumn"]["values"]
        counties65PlusNullIndex = data[4]["nullIndex"]
        counties65PlusPer = data[5]["doubleColumn"]["values"]
        counties65PlusPerNullIndex = data[5]["nullIndex"]

        # sorting null index lists so there is no chance of an index out of bounds error when we insert 0 values
        counties65PlusNullIndex.sort()
        counties65PlusPerNullIndex.sort()

        # inserting values of 0 for the indexes corresponding with the null value indexes
        for value in counties65PlusNullIndex:
            counties65plus.insert(value, 0)
        for value in counties18PlusPerNullIndex:
            counties65PlusPer.insert(value, 0)

        # create dose 2 dataframe
        countyVaccineDataFrame65 = pd.DataFrame(
            {
                "location_name": counties65plus,
                "TotalPer": counties65PlusPer,
            }
        )
        countyVaccineDataFrame65["age"] = "65_plus"
        countyVaccineDataFrame65["TotalPer"] = (
            countyVaccineDataFrame65["TotalPer"] * 100
        )

        # merge data frame for all populations with dataframe of those 18 +
        countyVaccineDataFrame = countyVaccineDataFrameAll.append(
            countyVaccineDataFrame18
        )

        # Merge Previous dataframe with dataframe of those 65+
        countyVaccineDataFrame = countyVaccineDataFrame.append(countyVaccineDataFrame65)

        countyVaccineDataFrame["dt"] = self.execution_dt

        crename = {
            "TotalPer": CMU(
                category="total_vaccine_completed",
                measurement="current",
                unit="percentage",
            ),
        }
        out = countyVaccineDataFrame.melt(
            id_vars=["dt", "location_name", "age"], value_vars=crename.keys()
        ).dropna()

        out["value"] = out["value"].astype(int)
        out["vintage"] = self._retrieve_vintage()
        df = self.extract_CMU(
            out,
            crename,
            ["category", "measurement", "unit", "sex", "race", "ethnicity"],
        )
        return df.drop(["variable"], axis="columns")
