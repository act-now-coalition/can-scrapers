import us
import pandas as pd

from can_tools.scrapers.base import CMU
from can_tools.scrapers.official.base import StateDashboard, TableauMapClick


class ArizonaStateData(TableauMapClick, StateDashboard):
    """
    Fetch county level covid data from Arizona's Tableau dashboard
    """

    # Initialize
    source = "https://www.azdhs.gov/preparedness/epidemiology-disease-control/infectious-disease-epidemiology/covid-19/dashboards/index.php"
    has_location = True
    location_type = "state"
    state_fips = int(us.states.lookup("Arizona").fips)

    def fetch(self):
        # Initialize
        outDf = pd.DataFrame()
        reqParams = {":embed": "y", ":display_count": "no"}
        url = "https://tableau.azdhs.gov/views/COVID-19Summary/Overview2"
        tbsroot = "https://tableau.azdhs.gov"

        info, fdat = self.getRawTbluPageData(url, tbsroot, reqParams)

        # Get the state data:
        outDf = outDf.append(
            self.extractTbluData(fdat, self.state_fips), ignore_index=True
        )

        outDf["CumPosTests"] = outDf["PercentPositive"] * outDf["Number of tests"]
        outDf["CumDiagPosTests"] = (
            outDf["Percent Positive Diagnostic tests"]
            * outDf["Number of Diagnostic tests"]
        )

        # NOTE: There is currently a bug in the AZDHS dashboard summary page. They do NOT show correct values for antibody positivity rate
        outDf["CumSeroPosTests"] = outDf["CumPosTests"] - outDf["CumDiagPosTests"]
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
            "category",
            "measurement",
            "unit",
            "age",
            "race",
            "sex",
            "value",
        ]
        return out.loc[:, cols_to_keep]
