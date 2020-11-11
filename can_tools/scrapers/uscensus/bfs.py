import pandas as pd

from .. import DatasetBaseNoDate, InsertWithTempTable

BASEURL = "https://www.census.gov"
DATEURL = "https://www.census.gov/econ/bfs/csv/date_table.csv"


class CensusBFS(InsertWithTempTable, DatasetBaseNoDate):
    """
    The Business Formation Statistics (BFS) are an experimental data
    product of the U.S. Census Bureau developed in research collaboration
    with economists affiliated with Board of Governors of the Federal
    Reserve System, Federal Reserve Bank of Atlanta, University of
    Maryland, and University of Notre Dame. The BFS provide timely and
    high frequency information on new business applications and
    formations in the United States. More information on Census Bureau
    Experimental Data products can be found
    [here](https://www.census.gov/data/experimental-data-products.html).

    For more information on what this data includes, please refer to the
    [Cenus site](https://www.census.gov/econ/bfs/index.html?#) or the
    [data dictionary](https://www.census.gov/econ/bfs/pdf/bfs_weekly_data_dictionary.pdf)

    Parameters
    ----------
    geo : str
        Indicates which geography that you'd like the data for. Can
        either be "us", "region", or "state"

    """

    pks = {
        "us": ("year", "week"),
        "region": ("region", "year", "week"),
        "state": ("state", "year", "week"),
    }

    def __init__(self, geo: str):
        self.geo = geo.lower()
        self.url = BASEURL + f"/econ/bfs/csv/bfs_{self.geo}_apps_weekly_nsa.csv"
        self.pk = self.pks[self.geo]
        self.table_name = f"bfs_{self.geo}"

    def get(self):
        # Download the data and the corresponding dates
        df = pd.read_csv(self.url)
        dates = pd.read_csv(DATEURL, parse_dates=["Start date", "End date"])

        # Merge the data and dates and rename columns
        out = df.merge(dates, on=["Year", "Week"], how="left")
        out = out.rename(columns={"Start date": "week_start", "End date": "week_end",})
        out.columns = [c.lower() for c in out.columns]

        return out
