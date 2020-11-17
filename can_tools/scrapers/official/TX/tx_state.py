import pandas as pd
import us

from can_tools.scrapers import DatasetBaseNoDate, CMU
from can_tools.scrapers.official.base import ArcGIS


class Texas(DatasetBaseNoDate, ArcGIS):
    """
    Get cases, deaths, and testing data on all TX counties from
    the TX ArcGIS dashboard
    """

    ARCGIS_ID = "ACaLB9ifngzawspq"
    source = (
        "https://txdshs.maps.arcgis.com/apps/opsdashboard/index.html"
        "#/ed483ecd702b4298ab01e8b9cafc8b83"
    )
    state_fips = int(us.states.lookup("Texas").fips)
    has_location = False

    def get_county_to_location(self) -> dict:
        """

        Returns
        -------

        TODO: fill me in
        """
        counties = self.get_all_sheet_to_df("tx_cnty", 0, 5).rename(
            columns={"CO_NAME": "county", "CO_FIPS": "location"}
        )

        county_to_location = {x.county: x.location for x in counties.itertuples()}

        return county_to_location

    def get_cases_deaths(self) -> pd.DataFrame:
        """
        Fetch county level cases and deaths data

        Returns
        -------
        df: pd.DataFrame
            pandas DataFrame containing data on cases and deaths
            for all counties in TX

        """

        # Load data and rename county/convert date
        df = self.get_all_sheet_to_df("DSHS_COVID19_Cases_Service", 0, 5).rename(
            columns={"County": "county"}
        )
        df["dt"] = self._retrieve_dt("US/Central")

        #
        crename = {
            "Positive": CMU(category="cases", measurement="cumulative", unit="people"),
            "Fatalities": CMU(
                category="deaths", measurement="cumulative", unit="people"
            ),
        }

        # Put into long format
        out = df.melt(id_vars=["county", "dt"], value_vars=crename.keys()).dropna()
        out["value"] = out["value"].astype(int)

        # Extract category information and add other variable context
        out = self.extract_CMU(out, crename)

        cols_to_keep = [
            "dt",
            "county",
            "category",
            "measurement",
            "unit",
            "age",
            "race",
            "sex",
            "value",
        ]

        return out.loc[:, cols_to_keep]

    def get_tests(self) -> pd.DataFrame:
        """

        Returns
        -------
        df: pd.DataFrame
            DataFrame containing pcr and antibody test counts for each county

        """

        df = self.get_all_sheet_to_df("DSHS_COVID19_TestData_Service", 0, 5).rename(
            columns={"County": "county"}
        )
        df["dt"] = self._retrieve_dt("US/Central")

        crename = {
            "ViralTest": CMU(
                category="pcr_tests_total", measurement="cumulative", unit="specimens"
            ),
            "AntibodyTe": CMU(
                category="antibody_tests_total",
                measurement="cumulative",
                unit="specimens",
            ),
            "Cumulative": CMU(
                category="unspecified_tests_total",
                measurement="cumulative",
                unit="unknown",
            ),
        }
        out = df.melt(
            id_vars=["county", "dt"],
            value_vars=list(crename.keys()),
        ).dropna()
        out["value"] = pd.to_numeric(out["value"])

        # Extract category information and add other variable context
        out = self.extract_CMU(out, crename)

        cols_to_keep = [
            "dt",
            "county",
            "category",
            "measurement",
            "unit",
            "age",
            "race",
            "sex",
            "value",
        ]

        return out.loc[:, cols_to_keep]

    def get(self) -> pd.DataFrame:
        """
        Get TX county level data

        Returns
        -------
        df: pd.DataFrame
            DataFrame with county level tests, cases, and deaths data

        """
        # Get mapping from county to FIPS codes
        cdra = self.get_cases_deaths()
        test = self.get_tests()

        out = pd.concat([cdra, test], axis=0, ignore_index=True)
        out["vintage"] = self._retrieve_vintage()

        return out
