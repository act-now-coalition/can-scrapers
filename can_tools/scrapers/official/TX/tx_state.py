from typing import Any, Dict
import pandas as pd
import us

from can_tools.scrapers import CMU
from can_tools.scrapers.official.base import ArcGIS


class TexasCasesDeaths(ArcGIS):
    """
    Get cases and deaths data on all TX counties from the TX ArcGIS dashboard
    """

    ARCGIS_ID = "ACaLB9ifngzawspq"
    source = (
        "https://txdshs.maps.arcgis.com/apps/opsdashboard/index.html"
        "#/ed483ecd702b4298ab01e8b9cafc8b83"
    )
    state_fips = int(us.states.lookup("Texas").fips)
    has_location = False
    service: str = "DSHS_COVID19_Cases_Service"
    crename: Dict[str, CMU] = {
        "Positive": CMU(category="cases", measurement="cumulative", unit="people"),
        "Fatalities": CMU(category="deaths", measurement="cumulative", unit="people"),
    }
    location_type = "county"

    def fetch(self) -> Any:
        return self.get_all_jsons(self.service, 0, 5)

    def normalize(self, data: Any) -> pd.DataFrame:
        """
        Fetch county level cases and deaths data

        Returns
        -------
        df: pd.DataFrame
            pandas DataFrame containing data on cases and deaths
            for all counties in TX

        """
        # Load data and rename county/convert date
        df = self.arcgis_jsons_to_df(data).rename(columns={"County": "location_name"})
        df["dt"] = self._retrieve_dt("US/Central")

        # Put into long format
        out = df.melt(
            id_vars=["location_name", "dt"], value_vars=self.crename.keys()
        ).dropna()
        out["value"] = out["value"].astype(int)
        out["vintage"] = self._retrieve_vintage()

        # Extract category information and add other variable context
        out = self.extract_CMU(out, self.crename)

        cols_to_keep = [
            "vintage",
            "dt",
            "location_name",
            "category",
            "measurement",
            "unit",
            "age",
            "race",
            "sex",
            "value",
        ]

        return out.loc[:, cols_to_keep]


class TexasTests(TexasCasesDeaths):
    """
    Get  testing data on all TX counties from the TX ArcGIS dashboard
    """

    service: str = "DSHS_COVID19_TestData_Service"
    crename: Dict[str, CMU] = {
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
