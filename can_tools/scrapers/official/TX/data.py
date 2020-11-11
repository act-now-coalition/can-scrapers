import textwrap

import pandas as pd
import us

from ... import DatasetBaseNoDate
from ..base import CMU, ArcGIS


class TXCounty(DatasetBaseNoDate, ArcGIS):
    ARCGIS_ID = "ACaLB9ifngzawspq"
    source = (
        "https://txdshs.maps.arcgis.com/apps/opsdashboard/index.html"
        "#/ed483ecd702b4298ab01e8b9cafc8b83"
    )
    state_fips = int(us.states.lookup("Texas").fips)
    has_location = False

    def get_county_to_location(self):
        counties = self.get_all_sheet_to_df("tx_cnty", 0, 5).rename(
            columns={
                "CO_NAME": "county",
                "CO_FIPS": "location"
            }
        )

        county_to_location = {
            x.county: x.location for x in counties.itertuples()
        }

        return county_to_location

    def get_cases_deaths(self):

        # Load data and rename county/convert date
        df = self.get_all_sheet_to_df("DSHS_COVID19_Cases_Service", 0, 5).rename(
            columns={"County": "county"}
        )
        df["dt"] = self._retrieve_dt("US/Central")

        #
        crename = {
            "Positive": CMU(
                category="cases",
                measurement="cumulative",
                unit="people"
            ),
            "Fatalities": CMU(
                category="deaths",
                measurement="cumulative",
                unit="people"
            )
        }

        # Put into long format
        out = df.melt(
            id_vars=["county", "dt"], value_vars=crename.keys()
        ).dropna()
        out["value"] = out["value"].astype(int)

        # Extract category information and add other variable context
        out = self.extract_cat_measurement_unit(out, crename)

        cols_to_keep = [
            "dt", "county", "category", "measurement", "unit",
            "age", "race", "sex", "value"
        ]

        return out.loc[:, cols_to_keep]

    def get_tests(self):
        df = self.get_all_sheet_to_df("DSHS_COVID19_TestData_Service", 0, 5).rename(
            columns={"County": "county"}
        )
        df["dt"] = self._retrieve_dt("US/Central")

        crename = {
            "ViralTest": CMU(
                category="pcr_tests_total",
                measurement="cumulative",
                unit="specimens"
            ),
            "AntibodyTe": CMU(
                category="antibody_tests_total",
                measurement="cumulative",
                unit="specimens"
            ),
            "Cumulative": CMU(
                category="unspecified_tests_total",
                measurement="cumulative",
                unit="unknown"
            )
        }
        out = df.melt(
            id_vars=["county", "dt"], value_vars=["ViralTest", "AntibodyTe", "Cumulative"]
        ).dropna()
        out["value"] = pd.to_numeric(out["value"])

        # Extract category information and add other variable context
        out = self.extract_cat_measurement_unit(out, crename)

        cols_to_keep = [
            "dt", "county", "category", "measurement", "unit",
            "age", "race", "sex", "value"
        ]

        return out.loc[:, cols_to_keep]

    def get(self):
        # Get mapping from county to FIPS codes
        c2l = self.get_county_to_location()

        cdra = self.get_cases_deaths()
        test = self.get_tests()

        out = pd.concat([cdra, test], axis=0, ignore_index=True)
        out["vintage"] = self._retrieve_vintage()

        return out
