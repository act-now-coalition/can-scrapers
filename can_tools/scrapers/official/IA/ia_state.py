import pandas as pd

import us
from can_tools.scrapers import CMU
from can_tools.scrapers.official.base import ArcGIS

from typing import Any


class IowaCasesDeaths(ArcGIS):
    """
    Fetch county level covid data from Iowa's ARCGIS dashboard
    """

    ARCGIS_ID = "vPD5PVLI6sfkZ5E4"
    has_location = False
    location_type = "county"
    state_fips = int(us.states.lookup("Iowa").fips)
    source = "https://coronavirus.iowa.gov/pages/rmcc-data"
    service: str = "IA_COVID19_Cases"

    cols_to_keep = [
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


class IowaHospitals(IowaCasesDeaths):
    service: str = "COVID19_RMCC_Hospitalization"
