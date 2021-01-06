import requests
import urllib.parse
from bs4 import BeautifulSoup
from can_tools.scrapers.base import CMU
from can_tools.scrapers.official.base import StateDashboard
import pandas as pd
import json
import re


class ArizonaStateData(StateDashboard):
    """
    Fetch county level covid data from Arizona's Tableau dashboard
    """

    # Initialize
    source = "https://www.azdhs.gov/preparedness/epidemiology-disease-control/infectious-disease-epidemiology/covid-19/dashboards/index.php"
    has_location = True
    location_type = "state"
    state_fips = 4
    cntys = [
        ["APACHE", 4001],
        ["COCHISE", 4003],
        ["COCONINO", 4005],
        ["GILA", 4007],
        ["GRAHAM", 4009],
        ["GREENLEE", 4011],
        ["LA PAZ", 4012],
        ["MARICOPA", 4013],
        ["MOHAVE", 4015],
        ["NAVAJO", 4017],
        ["PIMA", 4019],
        ["PINAL", 4021],
        ["SANTA CRUZ", 4023],
        ["YAVAPAI", 4025],
        ["YUMA", 4027],
    ]
    # Short list of request parameters for state-level db
    reqParams = {":embed": "y", ":display_count": "no"}

    def fetch(self):
        outDf = pd.DataFrame()
        # Extracts the map filter function, used to build tableau subview URLs
        def getMapFilter(htmDump):
            urlFltr = []
            # Grab the map filter function guts:
            for fn in htmDump["worldUpdate"]["applicationPresModel"][
                "workbookPresModel"
            ]["dashboardPresModel"]["userActions"]:
                if fn.get("name") == "Map filter":
                    urlFltr = (
                        urllib.parse.unquote(fn.get("linkSpec").get("url"))
                        .split("?")[1]
                        .replace("=<Countynm1~na>", "")
                        .split("&")
                    )
            return urlFltr

        def extractData(htmdump, area):  # Extracts data from raw tableau HTML bootstrap
            valF = []  # Initialize placeholder array
            # Grab the raw data loaded into the current tableau view
            lsUpdt = htmdump["secondaryInfo"]["presModelMap"]["dataDictionary"][
                "presModelHolder"
            ]["genDataDictionaryPresModel"]["dataSegments"]["0"]["dataColumns"][2][
                "dataValues"
            ][
                -1
            ]
            intDat = htmdump["secondaryInfo"]["presModelMap"]["dataDictionary"][
                "presModelHolder"
            ]["genDataDictionaryPresModel"]["dataSegments"]["0"]["dataColumns"][0][
                "dataValues"
            ]
            rlDat = htmdump["secondaryInfo"]["presModelMap"]["dataDictionary"][
                "presModelHolder"
            ]["genDataDictionaryPresModel"]["dataSegments"]["0"]["dataColumns"][1][
                "dataValues"
            ]
            # First extract the datatype and indices:
            for i in htmdump["secondaryInfo"]["presModelMap"]["vizData"][
                "presModelHolder"
            ]["genPresModelMapPresModel"]["presModelMap"]:
                dtyp = htmdump["secondaryInfo"]["presModelMap"]["vizData"][
                    "presModelHolder"
                ]["genPresModelMapPresModel"]["presModelMap"][i]["presModelHolder"][
                    "genVizDataPresModel"
                ][
                    "paneColumnsData"
                ][
                    "vizDataColumns"
                ][
                    1
                ].get(
                    "dataType"
                )
                indx = htmdump["secondaryInfo"]["presModelMap"]["vizData"][
                    "presModelHolder"
                ]["genPresModelMapPresModel"]["presModelMap"][i]["presModelHolder"][
                    "genVizDataPresModel"
                ][
                    "paneColumnsData"
                ][
                    "paneColumnsList"
                ][
                    0
                ][
                    "vizPaneColumns"
                ][
                    1
                ].get(
                    "aliasIndices"
                )[
                    0
                ]
                if dtyp == "integer":
                    valF.append([area, i, intDat[indx]])
                elif dtyp == "real":
                    valF.append([area, i, rlDat[indx]])
            valF.append([area, "Last update", lsUpdt])
            if valF:
                val = pd.DataFrame(valF, columns=["location", "Name", "Value"])
                val = pd.pivot_table(
                    val,
                    values="Value",
                    index=["location"],
                    columns="Name",
                    aggfunc="first",
                ).reset_index()
                return val
            else:
                return None

        # Let's start at the state-level
        url = "https://tableau.azdhs.gov/views/COVID-19Summary/Overview2"
        # Initialize main page: grab session ID key, sheet ID key, root directory string
        r = requests.get(url, params=self.reqParams)

        # Parse the output, return a json so we can build a bootstrap call
        suppe = BeautifulSoup(r.text, "html.parser")
        tdata = json.loads(suppe.find("textarea", {"id": "tsConfigContainer"}).text)

        # Call the bootstrapper: grab the state data, map selection update function
        dataUrl = f'https://tableau.azdhs.gov{tdata["vizql_root"]}/bootstrapSession/sessions/{tdata["sessionid"]}'
        r = requests.post(
            dataUrl,
            data={"sheet_id": tdata["sheetId"], "showParams": tdata["showParams"]},
        )

        # Regex the non-json output
        dat = re.search("\d+;({.*})\d+;({.*})", r.text, re.MULTILINE)

        # load info head and data group separately
        info = json.loads(dat.group(1))
        fdat = json.loads(dat.group(2))

        # Get the county filter url params
        cntFltr = getMapFilter(info)

        # Get the state data:
        outDf = outDf.append(extractData(fdat, self.state_fips), ignore_index=True)

        if cntFltr and self.location_type == "county":
            for county in self.cntys:
                cntyReqParam = self.reqParams
                for li in cntFltr:
                    cntyReqParam[li] = county[0]
                # You should be able to access an unlisted county-level tableau page at [r.url]
                r = requests.get(url, params=cntyReqParam)
                # Parse the output, return a json so we can build a bootstrap call
                suppe = BeautifulSoup(r.text, "html.parser")
                tdata = json.loads(
                    suppe.find("textarea", {"id": "tsConfigContainer"}).text
                )
                # Call the bootstrapper: grab the state data, map selection update function
                dataUrl = f'https://tableau.azdhs.gov{tdata["vizql_root"]}/bootstrapSession/sessions/{tdata["sessionid"]}'
                r = requests.post(
                    dataUrl,
                    data={
                        "sheet_id": tdata["sheetId"],
                        "showParams": tdata["showParams"],
                    },
                )
                # Regex the non-json output
                dat = re.search("\d+;({.*})\d+;({.*})", r.text, re.MULTILINE)
                # load info head and data group separately
                info = json.loads(dat.group(1))
                fdat = json.loads(dat.group(2))
                # Get county data
                cy = extractData(fdat, county[1])
                outDf = outDf.append(cy, ignore_index=True)

        outDf["CumPosTests"] = outDf["PercentPositive"] * outDf["Number of tests"]
        outDf["CumDiagPosTests"] = (
            outDf["Percent Positive Diagnostic tests"]
            * outDf["Number of Diagnostic tests"]
        )

        # NOTE: There is currently a bug in the AZDHS dashboard summary page. They do NOT show correct values for antibody positivity rate
        outDf["CumSeroPosTests"] = outDf["CumPosTests"] - outDf["CumDiagPosTests"]
        if self.location_type == "county":
            outDf = outDf[outDf["location"] != 4]
        return outDf

    def normalize(self, data):
        df = data
        crename = {
            "New Cases": CMU(category="cases", measurement="new", unit="people"),
            "New Deaths": CMU(category="deaths", measurement="new", unit="people"),
            "New Tested": CMU(
                category="unspecified_tests_total", measurement="new", unit="specimens"
            ),
            "New Tested PCR": CMU(
                category="antigen_pcr_tests_total", measurement="new", unit="specimens"
            ),
            "New Tested serology": CMU(
                category="antibody_tests_total", measurement="new", unit="specimens"
            ),
            "Number of Cases": CMU(
                category="cases", measurement="cumulative", unit="people"
            ),
            "Number of Diagnostic tests": CMU(
                category="antigen_pcr_tests_total",
                measurement="cumulative",
                unit="specimens",
            ),
            "Number of deaths": CMU(
                category="deaths", measurement="cumulative", unit="people"
            ),
            "Number of tests": CMU(
                category="unspecified_tests_total",
                measurement="cumulative",
                unit="specimens",
            ),
            "Number of tests serology": CMU(
                category="antibody_tests_total",
                measurement="cumulative",
                unit="specimens",
            ),
            "CumPosTests": CMU(
                category="unspecified_tests_positive",
                measurement="cumulative",
                unit="specimens",
            ),
            "CumDiagPosTests": CMU(
                category="antigen_pcr_tests_positive",
                measurement="cumulative",
                unit="specimens",
            ),
            "CumSeroPosTests": CMU(
                category="antibody_tests_positive",
                measurement="cumulative",
                unit="specimens",
            ),
        }

        out = (
            df.melt(id_vars=["location"], value_vars=crename.keys())
            .assign(
                dt=self._retrieve_dt("US/Eastern"), vintage=self._retrieve_vintage()
            )
            .dropna()
        )
        out.loc[:, "value"] = pd.to_numeric(out["value"])
        out.rename(columns={"Name": "variable"}, inplace=True)
        # Extract category information and add other variable context
        out = self.extract_CMU(out, crename)

        cols_to_keep = [
            "vintage",
            "dt",
            "location",
            "category",
            "measurement",
            "unit",
            "age",
            "race",
            "sex",
            "value",
        ]
        return out.loc[:, cols_to_keep]
