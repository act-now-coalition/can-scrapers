import pandas as pd
import us

from can_tools.scrapers.base import CMU
from can_tools.scrapers.official.base import StateDashboard, TableauMapClick


class ArizonaData(TableauMapClick, StateDashboard):
    """
    Fetch county level covid data from Arizona's Tableau dashboard
    """

    # Initlze
    source = "https://www.azdhs.gov/preparedness/epidemiology-disease-control/infectious-disease-epidemiology/covid-19/dashboards/index.php"
    has_location = True
    location_type = ""
    state_fips = int(us.states.lookup("Arizona").fips)
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

    def fetch(self):
        # Initialize
        dfs = []
        reqParams = {":embed": "y", ":display_count": "no"}
        url = "https://tableau.azdhs.gov/views/COVID-19Summary/Overview2"
        tbsroot = "https://tableau.azdhs.gov"

        info, fdat = self.getRawTbluPageData(url, tbsroot, reqParams)

        # Get the state data
        _df = self.extractTbluData(fdat, self.state_fips)
        _df["location_type"] = "state"
        dfs.append(_df)

        # Get the county filter url params
        cntFltr = self.getTbluMapFilter(info)

        if cntFltr:
            for county in self.cntys:
                cntyReqParam = reqParams
                for li in cntFltr:
                    cntyReqParam[li] = county[0]
                info, fdat = self.getRawTbluPageData(url, tbsroot, cntyReqParam)

                # Get county data
                _df = self.extractTbluData(fdat, county[1])
                _df["location_type"] = "county"

                dfs.append(_df)

            # Concat the dfs
            outDf = pd.concat(dfs, axis=0, ignore_index=True)

        outDf["CumPosTests"] = outDf["PercentPositive"] * outDf["Number of tests"]
        outDf["CumDiagPosTests"] = (
            outDf["Percent Positive Diagnostic tests"]
            * outDf["Number of Diagnostic tests"]
        )

        # NOTE: There is currently a bug in the AZDHS dashboard summary page. They do NOT show correct values for antibody positivity rate
        outDf["CumSeroPosTests"] = outDf["CumPosTests"] - outDf["CumDiagPosTests"]

        return outDf

    def normalize(self, data):
        df = data.copy()
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
                dt=self._retrieve_dt("US/Arizona"), vintage=self._retrieve_vintage()
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
            "location_type",
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
