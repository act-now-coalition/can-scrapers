import json
from datetime import datetime
from typing import Any

import pandas as pd
import us

from can_tools.scrapers.base import CMU, DatasetBase
from can_tools.scrapers.official.base import GoogleDataStudioDashboard


class WYStateVaccinations(GoogleDataStudioDashboard, DatasetBase):
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
    bodyDose1 = '{"dataRequest":[{"requestContext":{"reportContext":{"reportId":"30f32fc5-970a-4943-994d-6cccaea3c04f","pageId":"26374700","mode":"VIEW","componentId":"cd-kv0749i1fc","displayType":"simple-linechart"}},"datasetSpec":{"dataset":[{"datasourceId":"a51cf808-9bf3-44a0-bd26-4337aa9f8700","revisionNumber":0,"parameterOverrides":[]}],"queryFields":[{"name":"qt_r5yv4qgegc","datasetNs":"d0","tableNs":"t0","dataTransformation":{"sourceFieldName":"_2122702_"}},{"name":"qt_u0od8pekhc","datasetNs":"d0","tableNs":"t0","dataTransformation":{"sourceFieldName":"_1627270605_","aggregation":6}},{"name":"qt_l22kaqekhc","datasetNs":"d0","tableNs":"t0","dataTransformation":{"sourceFieldName":"_1817299283_","aggregation":6}}],"sortData":[{"sortColumn":{"name":"qt_r5yv4qgegc","datasetNs":"d0","tableNs":"t0","dataTransformation":{"sourceFieldName":"_2122702_"}},"sortDir":0}],"includeRowsCount":false,"blendConfig":{"blockDatasource":{"datasourceBlock":{"id":"block_84qwqa3xhc","type":1,"inputBlockIds":[],"outputBlockIds":[],"fields":[]},"blocks":[{"id":"block_94qwqa3xhc","type":5,"inputBlockIds":[],"outputBlockIds":[],"fields":[],"queryBlockConfig":{"joinQueryConfig":{"joinKeys":[],"queries":[{"datasourceId":"a51cf808-9bf3-44a0-bd26-4337aa9f8700","concepts":[]}]}}}],"delegatedAccessEnabled":true,"isUnlocked":true,"isCacheable":false}},"filters":[],"features":[],"dateRanges":[{"startDate":20201201,"endDate":20210316,"dataSubsetNs":{"datasetNs":"d0","tableNs":"t0","contextNs":"c0"}}],"contextNsCount":1,"dateRangeDimensions":[{"name":"qt_ky7yhzgegc","datasetNs":"d0","tableNs":"t0","dataTransformation":{"sourceFieldName":"_2122702_"}}],"calculatedField":[],"needGeocoding":false,"geoFieldMask":[]},"useDataColumn":true}]}'
    bodyDose1 = json.loads(bodyDose1)
    bodyDose1["dataRequest"][0]["datasetSpec"]["dateRanges"][0][
        "endDate"
    ] = datetime.now().strftime("%Y%m%d")

    bodyDose2 = '{"dataRequest":[{"requestContext":{"reportContext":{"reportId":"30f32fc5-970a-4943-994d-6cccaea3c04f","pageId":"26374700","mode":"VIEW","componentId":"cd-e0oqrj07fc","displayType":"simple-linechart"}},"datasetSpec":{"dataset":[{"datasourceId":"a51cf808-9bf3-44a0-bd26-4337aa9f8700","revisionNumber":0,"parameterOverrides":[]}],"queryFields":[{"name":"qt_gzwanrgegc","datasetNs":"d0","tableNs":"t0","dataTransformation":{"sourceFieldName":"_2122702_"}},{"name":"qt_idejgqekhc","datasetNs":"d0","tableNs":"t0","dataTransformation":{"sourceFieldName":"_1137649425_","aggregation":6}},{"name":"qt_hrpcjqekhc","datasetNs":"d0","tableNs":"t0","dataTransformation":{"sourceFieldName":"_532447383_","aggregation":6}}],"sortData":[{"sortColumn":{"name":"qt_gzwanrgegc","datasetNs":"d0","tableNs":"t0","dataTransformation":{"sourceFieldName":"_2122702_"}},"sortDir":0}],"includeRowsCount":false,"blendConfig":{"blockDatasource":{"datasourceBlock":{"id":"block_s70wqa3xhc","type":1,"inputBlockIds":[],"outputBlockIds":[],"fields":[]},"blocks":[{"id":"block_t70wqa3xhc","type":5,"inputBlockIds":[],"outputBlockIds":[],"fields":[],"queryBlockConfig":{"joinQueryConfig":{"joinKeys":[],"queries":[{"datasourceId":"a51cf808-9bf3-44a0-bd26-4337aa9f8700","concepts":[]}]}}}],"delegatedAccessEnabled":true,"isUnlocked":true,"isCacheable":false}},"filters":[],"features":[],"dateRanges":[{"startDate":20201201,"endDate":20210316,"dataSubsetNs":{"datasetNs":"d0","tableNs":"t0","contextNs":"c0"}}],"contextNsCount":1,"dateRangeDimensions":[{"name":"qt_tfbpmzgegc","datasetNs":"d0","tableNs":"t0","dataTransformation":{"sourceFieldName":"_2122702_"}}],"calculatedField":[],"needGeocoding":false,"geoFieldMask":[]},"useDataColumn":true}]}'
    bodyDose2 = json.loads(bodyDose2)
    bodyDose2["dataRequest"][0]["datasetSpec"]["dateRanges"][0][
        "endDate"
    ] = datetime.now().strftime("%Y%m%d")

    def fetch(self):
        """
        Pulls Wyoming state vaccine data, cleans and up the Json Strig that is returned before returning that object as
        a list of dictionaries
        """

        WYVacDataDose1 = self.get_dataset(self.bodyDose1, url=self.baseUrl)
        WYVacDataDose1 = json.loads(WYVacDataDose1[10 : len(WYVacDataDose1) - 1])
        WYVacDataDose2 = self.get_dataset(self.bodyDose2, url=self.baseUrl)
        WYVacDataDose2 = json.loads(WYVacDataDose2[10 : len(WYVacDataDose2) - 1])
        # add data from the dose 2 request to our list of dictionaries
        df = WYVacDataDose1["default"]["dataResponse"][0]["dataSubset"][0]["dataset"][
            "tableDataset"
        ]["column"]
        df.append(
            WYVacDataDose2["default"]["dataResponse"][0]["dataSubset"][0]["dataset"][
                "tableDataset"
            ]["column"][0]
        )
        df.append(
            WYVacDataDose2["default"]["dataResponse"][0]["dataSubset"][0]["dataset"][
                "tableDataset"
            ]["column"][1]
        )
        df.append(
            WYVacDataDose2["default"]["dataResponse"][0]["dataSubset"][0]["dataset"][
                "tableDataset"
            ]["column"][2]
        )

        return df

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


class WYCountyVaccinations(GoogleDataStudioDashboard, DatasetBase):
    """
    Pulls Wyoming state vaccine data, cleans and up the Json Strig that is returned before returning that object as
    a dictionary
    """

    # JSONs to pass to API to get county level vaccine dosage informtation
    bodyDose1 = '{"dataRequest":[{"requestContext":{"reportContext":{"reportId":"30f32fc5-970a-4943-994d-6cccaea3c04f","pageId":"26374700","mode":"VIEW","componentId":"cd-7lx3egj1fc","displayType":"simple-barchart"}},"datasetSpec":{"dataset":[{"datasourceId":"a51cf808-9bf3-44a0-bd26-4337aa9f8700","revisionNumber":0,"parameterOverrides":[]}],"queryFields":[{"name":"qt_xbvwixgegc","datasetNs":"d0","tableNs":"t0","dataTransformation":{"sourceFieldName":"_2024258922_"}},{"name":"qt_npp0vqekhc","datasetNs":"d0","tableNs":"t0","dataTransformation":{"sourceFieldName":"_1632125930_","aggregation":6}},{"name":"qt_iepeyqekhc","datasetNs":"d0","tableNs":"t0","dataTransformation":{"sourceFieldName":"_n1186709724_","aggregation":6}}],"sortData":[{"sortColumn":{"name":"qt_jpyshgj1fc","datasetNs":"d0","tableNs":"t0","dataTransformation":{"sourceFieldName":"_2024258922_"}},"sortDir":0}],"includeRowsCount":true,"paginateInfo":{"startRow":1,"rowsCount":25},"blendConfig":{"blockDatasource":{"datasourceBlock":{"id":"block_p8twqa3xhc","type":1,"inputBlockIds":[],"outputBlockIds":[],"fields":[]},"blocks":[{"id":"block_q8twqa3xhc","type":5,"inputBlockIds":[],"outputBlockIds":[],"fields":[],"queryBlockConfig":{"joinQueryConfig":{"joinKeys":[],"queries":[{"datasourceId":"a51cf808-9bf3-44a0-bd26-4337aa9f8700","concepts":[]}]}}}],"delegatedAccessEnabled":true,"isUnlocked":true,"isCacheable":false}},"filters":[{"filterDefinition":{"filterExpression":{"include":false,"conceptType":0,"concept":{"ns":"t0","name":"qt_cn2odwj1fc"},"filterConditionType":"EQ","stringValues":["Wyoming"],"numberValues":[],"queryTimeTransformation":{"dataTransformation":{"sourceFieldName":"_2024258922_"}}}},"dataSubsetNs":{"datasetNs":"d0","tableNs":"t0","contextNs":"c0"},"version":3}],"features":[],"dateRanges":[],"contextNsCount":1,"calculatedField":[],"needGeocoding":false,"geoFieldMask":[]},"useDataColumn":true}]}'
    bodyDose2 = '{"dataRequest":[{"requestContext":{"reportContext":{"reportId":"30f32fc5-970a-4943-994d-6cccaea3c04f","pageId":"26374700","mode":"VIEW","componentId":"cd-fsb52lj1fc","displayType":"simple-barchart"}},"datasetSpec":{"dataset":[{"datasourceId":"a51cf808-9bf3-44a0-bd26-4337aa9f8700","revisionNumber":0,"parameterOverrides":[]}],"queryFields":[{"name":"qt_sop6lxgegc","datasetNs":"d0","tableNs":"t0","dataTransformation":{"sourceFieldName":"_2024258922_"}},{"name":"qt_fhpe6qekhc","datasetNs":"d0","tableNs":"t0","dataTransformation":{"sourceFieldName":"_n83790042_","aggregation":6}},{"name":"qt_25rc9qekhc","datasetNs":"d0","tableNs":"t0","dataTransformation":{"sourceFieldName":"_n1602653088_","aggregation":6}}],"sortData":[{"sortColumn":{"name":"qt_8jc52lj1fc","datasetNs":"d0","tableNs":"t0","dataTransformation":{"sourceFieldName":"_2024258922_"}},"sortDir":0}],"includeRowsCount":true,"paginateInfo":{"startRow":1,"rowsCount":25},"blendConfig":{"blockDatasource":{"datasourceBlock":{"id":"block_ekwwqa3xhc","type":1,"inputBlockIds":[],"outputBlockIds":[],"fields":[]},"blocks":[{"id":"block_fkwwqa3xhc","type":5,"inputBlockIds":[],"outputBlockIds":[],"fields":[],"queryBlockConfig":{"joinQueryConfig":{"joinKeys":[],"queries":[{"datasourceId":"a51cf808-9bf3-44a0-bd26-4337aa9f8700","concepts":[]}]}}}],"delegatedAccessEnabled":true,"isUnlocked":true,"isCacheable":false}},"filters":[{"filterDefinition":{"filterExpression":{"include":false,"conceptType":0,"concept":{"ns":"t0","name":"qt_cn2odwj1fc"},"filterConditionType":"EQ","stringValues":["Wyoming"],"numberValues":[],"queryTimeTransformation":{"dataTransformation":{"sourceFieldName":"_2024258922_"}}}},"dataSubsetNs":{"datasetNs":"d0","tableNs":"t0","contextNs":"c0"},"version":3}],"features":[],"dateRanges":[],"contextNsCount":1,"calculatedField":[],"needGeocoding":false,"geoFieldMask":[]},"useDataColumn":true}]}'
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
        parsedVacDataDose1 = json.loads(WYVacDataDose1[10 : len(WYVacDataDose1) - 1])
        WYVacDataDose2 = self.get_dataset(json.loads(self.bodyDose2), url=self.baseUrl)
        parsedVacDataDose2 = json.loads(WYVacDataDose2[10 : len(WYVacDataDose2) - 1])
        df = parsedVacDataDose1["default"]["dataResponse"][0]["dataSubset"][0][
            "dataset"
        ]["tableDataset"]["column"]
        df.append(
            parsedVacDataDose2["default"]["dataResponse"][0]["dataSubset"][0][
                "dataset"
            ]["tableDataset"]["column"][0]
        )
        df.append(
            parsedVacDataDose2["default"]["dataResponse"][0]["dataSubset"][0][
                "dataset"
            ]["tableDataset"]["column"][1]
        )
        df.append(
            parsedVacDataDose2["default"]["dataResponse"][0]["dataSubset"][0][
                "dataset"
            ]["tableDataset"]["column"][2]
        )
        return df

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
