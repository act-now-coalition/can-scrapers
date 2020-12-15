import pandas as pd
import us

from can_tools.scrapers import DatasetBase
from can_tools.scrapers.official.base import ArcGIS


class WisconsinArcGIS(ArcGIS, DatasetBase):
    """
    ArcGIS scraper that retrieves dashboard information for the
    state of Wisconsin (which has their own self-hosted ArcGIS
    instance)
    """

    ARCGIS_ID = ""

    def arcgis_query_url(
        self, service="DHS_COVID19/COVID19_WI", sheet=1, srvid="server"
    ):
        out = f"https://dhsgis.wi.gov/{srvid}/rest/services/{service}/MapServer/{sheet}/query"

        return out
