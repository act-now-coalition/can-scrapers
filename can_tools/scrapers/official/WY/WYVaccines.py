import json
from datetime import datetime
from typing import Any

import pandas as pd
import us
import jmespath

from can_tools.scrapers.base import CMU
from can_tools.scrapers.official.base import GoogleDataStudioDashboard


class WYStateVaccinations(GoogleDataStudioDashboard):
    state_fips = int(us.states.lookup("Wyoming").fips)
    execution_dt = pd.Timestamp.now()
    source = (
        "https://health.wyo.gov/publichealth/immunization/wyoming-covid-19-vaccine-information/covid-19-"
        "vaccine-distribution-data/"
    )
    source_name = "Wyoming Department of Health"
    baseUrl = "https://datastudio.google.com/batchedDataV2"
    resource_id = "a51cf808-9bf3-44a0-bd26-4337aa9f8700"
    has_location = True
    location_type = "state"
    # These Jsons are big, and very messy. I have not taken the time to go through and see what exactly in here is
    # necessary and what is 'fluff.' That is on my to do list
    bodyDose1 = '{"dataRequest":[{"requestContext":{"reportContext":{"reportId":"30f32fc5-970a-4943-994d-6cccaea3c04f","pageId":"26374700","mode":"VIEW","componentId":"cd-kv0749i1fc","displayType":"simple-linechart"}},"datasetSpec":{"dataset":[{"datasourceId":"1ad449b7-1654-4568-b8c2-b436564337fc","revisionNumber":0,"parameterOverrides":[]}],"queryFields":[{"name":"qt_rvc2lvf4hc","datasetNs":"d0","tableNs":"t0","dataTransformation":{"sourceFieldName":"_2122702_"}},{"name":"qt_c5rkvvf4hc","datasetNs":"d0","tableNs":"t0","dataTransformation":{"sourceFieldName":"_943306672_","aggregation":6}},{"name":"qt_6bb2lvf4hc","datasetNs":"d0","tableNs":"t0","dataTransformation":{"sourceFieldName":"_1336940214_","aggregation":6}}],"sortData":[{"sortColumn":{"name":"qt_rvc2lvf4hc","datasetNs":"d0","tableNs":"t0","dataTransformation":{"sourceFieldName":"_2122702_"}},"sortDir":0}],"includeRowsCount":false,"blendConfig":{"blockDatasource":{"datasourceBlock":{"id":"block_az4tkpckic","type":1,"inputBlockIds":[],"outputBlockIds":[],"fields":[]},"blocks":[{"id":"block_bz4tkpckic","type":5,"inputBlockIds":[],"outputBlockIds":[],"fields":[],"queryBlockConfig":{"joinQueryConfig":{"joinKeys":[],"queries":[{"datasourceId":"1ad449b7-1654-4568-b8c2-b436564337fc","concepts":[]}]}}}],"delegatedAccessEnabled":true,"isUnlocked":true,"isCacheable":false}},"filters":[],"features":[],"dateRanges":[],"contextNsCount":1,"dateRangeDimensions":[{"name":"qt_xl2mnvf4hc","datasetNs":"d0","tableNs":"t0","dataTransformation":{"sourceFieldName":"_2122702_"}}],"calculatedField":[],"needGeocoding":false,"geoFieldMask":[],"geoVertices":100000},"useDataColumn":true}]}'
    bodyDose1 = json.loads(bodyDose1)
    # bodyDose1["dataRequest"][0]["datasetSpec"]["dateRanges"][0][
    #     "endDate"
    # ] = datetime.now().strftime("%Y%m%d")

    bodyDose2 = '{"dataRequest":[{"requestContext":{"reportContext":{"reportId":"30f32fc5-970a-4943-994d-6cccaea3c04f","pageId":"26374700","mode":"VIEW","componentId":"cd-v0498vf4hc","displayType":"simple-linechart"}},"datasetSpec":{"dataset":[{"datasourceId":"1ad449b7-1654-4568-b8c2-b436564337fc","revisionNumber":0,"parameterOverrides":[]}],"queryFields":[{"name":"qt_w0498vf4hc","datasetNs":"d0","tableNs":"t0","dataTransformation":{"sourceFieldName":"_2122702_"}},{"name":"qt_bkpyfwf4hc","datasetNs":"d0","tableNs":"t0","dataTransformation":{"sourceFieldName":"_1137649425_","aggregation":6}},{"name":"qt_iovaiwf4hc","datasetNs":"d0","tableNs":"t0","dataTransformation":{"sourceFieldName":"_n316847366_","aggregation":6}}],"sortData":[{"sortColumn":{"name":"qt_w0498vf4hc","datasetNs":"d0","tableNs":"t0","dataTransformation":{"sourceFieldName":"_2122702_"}},"sortDir":0}],"includeRowsCount":false,"blendConfig":{"blockDatasource":{"datasourceBlock":{"id":"block_aidukpckic","type":1,"inputBlockIds":[],"outputBlockIds":[],"fields":[]},"blocks":[{"id":"block_bidukpckic","type":5,"inputBlockIds":[],"outputBlockIds":[],"fields":[],"queryBlockConfig":{"joinQueryConfig":{"joinKeys":[],"queries":[{"datasourceId":"1ad449b7-1654-4568-b8c2-b436564337fc","concepts":[]}]}}}],"delegatedAccessEnabled":true,"isUnlocked":true,"isCacheable":false}},"filters":[],"features":[],"dateRanges":[],"contextNsCount":1,"dateRangeDimensions":[{"name":"qt_z0498vf4hc","datasetNs":"d0","tableNs":"t0","dataTransformation":{"sourceFieldName":"_2122702_"}}],"calculatedField":[],"needGeocoding":false,"geoFieldMask":[],"geoVertices":100000},"useDataColumn":true}]}'
    bodyDose2 = json.loads(bodyDose2)
    # bodyDose2["dataRequest"][0]["datasetSpec"]["dateRanges"][0][
    #     "endDate"
    # ] = datetime.now().strftime("%Y%m%d")

    def fetch(self):
        """
        Pulls Wyoming state vaccine data, cleans and up the Json Strig that is returned before returning that object as
        a list of dictionaries
        """

        WYVacDataDose1 = self.get_dataset(self.bodyDose1, url=self.baseUrl)
        WYVacDataDose1 = json.loads(WYVacDataDose1[11:-1])
        WYVacDataDose2 = self.get_dataset(self.bodyDose2, url=self.baseUrl)
        WYVacDataDose2 = json.loads(WYVacDataDose2[11:-1])

        query = jmespath.compile(
            "dataResponse[0].dataSubset[0].dataset.tableDataset.column"
        )
        return query.search(WYVacDataDose1) + query.search(WYVacDataDose2)
        # add data from the dose 2 request to our list of dictionaries

    def normalize(self, data) -> pd.DataFrame:
        """
        Cleans and normalizes the data we recieve from the Google api
        """
        datesListVac1 = data[0]["dateColumn"]["values"]
        nullValuesSupplyListVac1 = data[1]["nullIndex"]
        valuesSupplyListVac1 = data[1]["doubleColumn"]["values"]
        nullValuesAdminListVac1 = data[2]["nullIndex"]
        valuesAdminListVac1 = data[2]["doubleColumn"]["values"]

        # sorts the Null Values index lists so we dont get an out of bounds error when we insert values of 0 in their places
        nullValuesAdminListVac1.sort()
        nullValuesSupplyListVac1.sort()
        for value in nullValuesSupplyListVac1:
            valuesSupplyListVac1.insert(value, 0)
        for value in nullValuesAdminListVac1:
            valuesAdminListVac1.insert(value, 0)

        # creating dataframe for dose 1 county level data
        stateVaccineDataFrameVac1 = pd.DataFrame(
            {
                "dt": datesListVac1,
                "supplyVac1": valuesSupplyListVac1,
                "administeredVac1": valuesAdminListVac1,
            }
        )

        datesListVac2 = data[3]["dateColumn"]["values"]
        nullValuesSupplyListVac2 = data[4]["nullIndex"]
        valuesSupplyListVac2 = data[4]["doubleColumn"]["values"]
        nullValuesAdminListVac2 = data[5]["nullIndex"]
        valuesAdminListVac2 = data[5]["doubleColumn"]["values"]
        nullValuesAdminListVac2.sort()
        nullValuesSupplyListVac2.sort()
        for value in nullValuesSupplyListVac2:
            valuesSupplyListVac2.insert(value, 0)
        for value in nullValuesAdminListVac2:
            valuesAdminListVac2.insert(value, 0)

        # creating data frame for second dose of vaccines
        stateVaccineDataFrameVac2 = pd.DataFrame(
            {
                "dt": datesListVac2,
                "supplyVac2": valuesSupplyListVac2,
                "administeredVac2": valuesAdminListVac2,
            }
        )

        # merges two data frames together
        stateVaccineDataFrame = stateVaccineDataFrameVac1.merge(
            stateVaccineDataFrameVac2, on="dt", how="outer"
        )
        # sums the first dose allocation and the second dose allocations together
        stateVaccineDataFrame["supplyTotal"] = (
            stateVaccineDataFrame["supplyVac2"] + stateVaccineDataFrame["supplyVac1"]
        )
        # create cumulative vaccine supply variable
        stateVaccineDataFrame["supplyCumulative"] = [
            stateVaccineDataFrame["supplyTotal"].loc[0:x].sum()
            for x in range(len(stateVaccineDataFrame["supplyTotal"]))
        ]
        stateVaccineDataFrame["location"] = self.state_fips
        stateVaccineDataFrame["dt"] = pd.to_datetime(stateVaccineDataFrame["dt"])
        crename = {
            "supplyCumulative": CMU(
                category="total_vaccine_allocated",
                measurement="cumulative",
                unit="doses",
            ),
            "administeredVac1": CMU(
                category="total_vaccine_initiated", measurement="new", unit="people"
            ),
            "administeredVac2": CMU(
                category="total_vaccine_completed", measurement="new", unit="people"
            ),
        }
        out = stateVaccineDataFrame.melt(
            id_vars=["dt", "location"], value_vars=crename.keys()
        ).dropna()
        out["value"] = out["value"].astype(int)
        out["vintage"] = self._retrieve_vintage()
        out = self.extract_CMU(out, crename)
        return out.drop(["variable"], axis="columns")


class WYCountyVaccinations(GoogleDataStudioDashboard):
    """
    Pulls Wyoming state vaccine data, cleans and up the Json Strig that is returned before returning that object as
    a dictionary
    """

    # JSONs to pass to API to get county level vaccine dosage informtation
    bodyDose1 = '{"dataRequest":[{"requestContext":{"reportContext":{"reportId":"30f32fc5-970a-4943-994d-6cccaea3c04f","pageId":"26374700","mode":"VIEW","componentId":"cd-7lx3egj1fc","displayType":"simple-barchart"}},"datasetSpec":{"dataset":[{"datasourceId":"1ad449b7-1654-4568-b8c2-b436564337fc","revisionNumber":0,"parameterOverrides":[]}],"queryFields":[{"name":"qt_mmzjd7f4hc","datasetNs":"d0","tableNs":"t0","dataTransformation":{"sourceFieldName":"_2024258922_"}},{"name":"qt_9eem98f4hc","datasetNs":"d0","tableNs":"t0","dataTransformation":{"sourceFieldName":"_n236049110_","aggregation":1}},{"name":"qt_ic77p9f4hc","datasetNs":"d0","tableNs":"t0","dataTransformation":{"sourceFieldName":"_n2032582864_","aggregation":1}}],"sortData":[{"sortColumn":{"name":"qt_mmzjd7f4hc","datasetNs":"d0","tableNs":"t0","dataTransformation":{"sourceFieldName":"_2024258922_"}},"sortDir":0}],"includeRowsCount":true,"paginateInfo":{"startRow":1,"rowsCount":25},"blendConfig":{"blockDatasource":{"datasourceBlock":{"id":"block_za7tkpckic","type":1,"inputBlockIds":[],"outputBlockIds":[],"fields":[]},"blocks":[{"id":"block_0a7tkpckic","type":5,"inputBlockIds":[],"outputBlockIds":[],"fields":[],"queryBlockConfig":{"joinQueryConfig":{"joinKeys":[],"queries":[{"datasourceId":"1ad449b7-1654-4568-b8c2-b436564337fc","concepts":[]}]}}}],"delegatedAccessEnabled":true,"isUnlocked":true,"isCacheable":false}},"filters":[{"filterDefinition":{"filterExpression":{"include":false,"conceptType":0,"concept":{"ns":"t0","name":"qt_cn2odwj1fc"},"filterConditionType":"EQ","stringValues":["Wyoming"],"numberValues":[],"queryTimeTransformation":{"dataTransformation":{"sourceFieldName":"_2024258922_"}}}},"dataSubsetNs":{"datasetNs":"d0","tableNs":"t0","contextNs":"c0"},"version":3}],"features":[],"dateRanges":[],"contextNsCount":1,"calculatedField":[],"needGeocoding":false,"geoFieldMask":[],"geoVertices":100000},"useDataColumn":true}]}'
    bodyDose2 = '{"dataRequest":[{"requestContext":{"reportContext":{"reportId":"30f32fc5-970a-4943-994d-6cccaea3c04f","pageId":"26374700","mode":"VIEW","componentId":"cd-x520hag4hc","displayType":"simple-barchart"}},"datasetSpec":{"dataset":[{"datasourceId":"1ad449b7-1654-4568-b8c2-b436564337fc","revisionNumber":0,"parameterOverrides":[]}],"queryFields":[{"name":"qt_y520hag4hc","datasetNs":"d0","tableNs":"t0","dataTransformation":{"sourceFieldName":"_2024258922_"}},{"name":"qt_98tnwag4hc","datasetNs":"d0","tableNs":"t0","dataTransformation":{"sourceFieldName":"_950921146_","aggregation":6}},{"name":"qt_u2iluag4hc","datasetNs":"d0","tableNs":"t0","dataTransformation":{"sourceFieldName":"_n1692847680_","aggregation":6}}],"sortData":[{"sortColumn":{"name":"qt_y520hag4hc","datasetNs":"d0","tableNs":"t0","dataTransformation":{"sourceFieldName":"_2024258922_"}},"sortDir":0}],"includeRowsCount":true,"paginateInfo":{"startRow":1,"rowsCount":25},"blendConfig":{"blockDatasource":{"datasourceBlock":{"id":"block_qhkukpckic","type":1,"inputBlockIds":[],"outputBlockIds":[],"fields":[]},"blocks":[{"id":"block_rhkukpckic","type":5,"inputBlockIds":[],"outputBlockIds":[],"fields":[],"queryBlockConfig":{"joinQueryConfig":{"joinKeys":[],"queries":[{"datasourceId":"1ad449b7-1654-4568-b8c2-b436564337fc","concepts":[]}]}}}],"delegatedAccessEnabled":true,"isUnlocked":true,"isCacheable":false}},"filters":[{"filterDefinition":{"filterExpression":{"include":false,"conceptType":0,"concept":{"ns":"t0","name":"qt_cn2odwj1fc"},"filterConditionType":"EQ","stringValues":["Wyoming"],"numberValues":[],"queryTimeTransformation":{"dataTransformation":{"sourceFieldName":"_2024258922_"}}}},"dataSubsetNs":{"datasetNs":"d0","tableNs":"t0","contextNs":"c0"},"version":3}],"features":[],"dateRanges":[],"contextNsCount":1,"calculatedField":[],"needGeocoding":false,"geoFieldMask":[],"geoVertices":100000},"useDataColumn":true}]}'
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
        WYVacDataDose1 = self.get_dataset(json.loads(self.bodyDose1), url=self.baseUrl)
        parsedDose1 = json.loads(WYVacDataDose1[11:-1])
        WYVacDataDose2 = self.get_dataset(json.loads(self.bodyDose2), url=self.baseUrl)
        parsedDose2 = json.loads(WYVacDataDose2[11:-1])

        query = jmespath.compile(
            "dataResponse[0].dataSubset[0].dataset.tableDataset.column"
        )
        return query.search(parsedDose1) + query.search(parsedDose2)

    def normalize(self, data) -> pd.DataFrame:
        countiesVac1 = data[0]["stringColumn"]["values"]
        countiesNullIndexVac1 = data[0]["nullIndex"]
        countiesSupplyVac1 = data[1]["doubleColumn"]["values"]
        countiesSupplyNullIndexVac1 = data[1]["nullIndex"]
        countiesAllocVac1 = data[2]["doubleColumn"]["values"]
        countiesAllocNullIndexVac1 = data[2]["nullIndex"]

        # sorting null index lists so there is no chance of an index out of bounds error when we insert 0 values for
        # null indexes
        countiesNullIndexVac1.sort()
        countiesAllocNullIndexVac1.sort()
        countiesSupplyNullIndexVac1.sort()

        # inserting values of 0 for the indexes corresponding with the null value indexes
        for value in countiesNullIndexVac1:
            countiesVac1.insert(value, 0)
        for value in countiesSupplyNullIndexVac1:
            countiesSupplyVac1.insert(value, 0)
        for value in countiesAllocNullIndexVac1:
            countiesAllocVac1.insert(value, 0)

        # create dose 1 data frame
        countyVaccineDataFrameVac1 = pd.DataFrame(
            {
                "location_name": countiesVac1,
                "supplyVac1": countiesSupplyVac1,
                "administeredVac1": countiesAllocVac1,
            }
        )

        countiesVac2 = data[3]["stringColumn"]["values"]
        countiesNullIndexVac2 = data[3]["nullIndex"]
        countiesSupplyVac2 = data[4]["doubleColumn"]["values"]
        countiesSupplyNullIndexVac2 = data[4]["nullIndex"]
        countiesAllocVac2 = data[5]["doubleColumn"]["values"]
        countiesAllocNullIndexVac2 = data[5]["nullIndex"]

        # sorting null index lists so there is no chance of an index out of bounds error when we insert 0 values
        countiesNullIndexVac2.sort()
        countiesAllocNullIndexVac2.sort()
        countiesSupplyNullIndexVac2.sort()

        # inserting values of 0 for the indexes corresponding with the null value indexes
        for value in countiesNullIndexVac2:
            countiesVac2.insert(value, 0)
        for value in countiesSupplyNullIndexVac2:
            countiesSupplyVac2.insert(value, 0)
        for value in countiesAllocNullIndexVac2:
            countiesAllocVac2.insert(value, 0)

        # create dose 2 dataframe
        countyVaccineDataFrameVac2 = pd.DataFrame(
            {
                "location_name": countiesVac2,
                "supplyVac2": countiesSupplyVac2,
                "administeredVac2": countiesAllocVac2,
            }
        )

        # merge dose 1 data frame with dose 2 dataframe
        countyVaccineDataFrame = countyVaccineDataFrameVac1.merge(
            countyVaccineDataFrameVac2, on="location_name", how="outer"
        )

        countyVaccineDataFrame["dt"] = self.execution_dt
        countyVaccineDataFrame["totalSupply"] = (
            countyVaccineDataFrame["supplyVac2"] + countyVaccineDataFrame["supplyVac1"]
        )

        crename = {
            "totalSupply": CMU(
                category="total_vaccine_allocated",
                measurement="cumulative",
                unit="doses",
            ),
            "administeredVac1": CMU(
                category="total_vaccine_initiated",
                measurement="cumulative",
                unit="people",
            ),
            "administeredVac2": CMU(
                category="total_vaccine_completed",
                measurement="cumulative",
                unit="people",
            ),
        }
        out = countyVaccineDataFrame.melt(
            id_vars=["dt", "location_name"], value_vars=crename.keys()
        ).dropna()

        out["value"] = out["value"].astype(int)
        out["vintage"] = self._retrieve_vintage()
        df = self.extract_CMU(out, crename)
        return df.drop(["variable"], axis="columns")


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
