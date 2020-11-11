import geopandas as gpd
import pandas as pd

from ..base import DatasetBaseNoDate, InsertWithTempTable
from .census import STATE_FIPS

BASE_GEO_URL = "https://www2.census.gov/geo/tiger/"


def _create_location(geo: str, df: pd.DataFrame):
    """
    Converts geographic columns into a fips code

    Parameters
    ----------
    df : pd.DataFrame
        The output of a `data_get` request and must include the
        relevant geographic columns

    Returns
    -------
    df : pd.DataFrame
        A DataFrame with the fips code values included and the
        other geographic columns dropped
    """
    df["location_type"] = geo
    if geo == "state":
        df["location"] = df["state"].astype(int)
    elif geo == "county":
        df["location"] = df["state"].astype(int) * 1_000 + df["county"].astype(int)
    else:
        raise ValueError("Only state/county are supported")

    return df


def _download_shape_file(apiurl: str, filename: str):
    # Create the url string geopandas needs to know that
    # it is a zip file
    rq_str = f"{apiurl}{filename}.zip"

    # Read shapefile
    gdf = gpd.read_file(rq_str)

    gdf = gdf.rename(
        columns={"STATEFP": "STATE", "COUNTYFP": "COUNTY", "TRACTCE": "TRACT"}
    )
    gdf["INTPTLAT"] = pd.to_numeric(gdf["INTPTLAT"])
    gdf["INTPTLON"] = pd.to_numeric(gdf["INTPTLON"])
    gdf.columns = [c.lower() for c in gdf.columns]

    return gdf


def download_shape_files(geo: str, year: int):
    """
    Downloads the shape files for a particular geography and year.

    The code currently only accepts state, county, and tract as the
    possible values for `geo`

    Parameters
    ----------
    geo : str
        The geography to download
    year : int
        The year of geography definitions to reference

    Returns
    -------
    gdf : pandas.DataFrame
        A DataFrame with information about the specified geography
    """
    geo = geo.lower()
    url = BASE_GEO_URL + f"TIGER{year}/{geo.upper()}/"

    datafile = f"tl_{year}_us_{geo}"
    gdf = _download_shape_file(url, datafile)
    gdf = _create_location(geo, gdf)

    keep = ["location", "location_type", "state", "aland", "intptlat", "intptlon"]
    if geo == "county":
        keep.append("namelsad")
        gdf = gdf.loc[:, keep].rename(columns={"namelsad": "fullname"})
        gdf["name"] = gdf["fullname"].str.replace(" County", "")
    else:
        keep.append("name")
        gdf = gdf.loc[:, keep]
        gdf["fullname"] = gdf["name"]

    # Convert land area to square miles (m^2 -> km^2 -> mi^2
    gdf["aland"] = (gdf["aland"] / 1_000_000) / 2.5899

    # Remove 'County' from the county names -- We need namelsad because
    # it allows us to differentiate between places like St. Louis county
    # and St. Louis City county...
    gdf = gdf.rename(
        columns={"aland": "area", "intptlat": "latitude", "intptlon": "longitude",}
    )

    return gdf


class USGeoBaseAPI(InsertWithTempTable, DatasetBaseNoDate):
    table_name = "locations"
    pk = '("id")'
    autodag = False

    def __init__(self, geo: str = "state", year: int = 2019):
        self.geo = geo
        self.year = year

    def _insert_query(self, df: pd.DataFrame, table_name: str, temp_name: str, pk: str):
        _sql_geo_insert = f"""
        INSERT INTO meta.{table_name} (location, location_type, state, name, area, latitude, longitude, fullname)
        SELECT tt.location, loct.id, tt.state, tt.name,
               tt.area, tt.latitude, tt.longitude, tt.fullname
        FROM {temp_name} tt
        LEFT JOIN meta.location_type loct ON loct.name=tt.location_type
        ON CONFLICT (location) DO NOTHING;
        """

        return _sql_geo_insert

    def get(self):
        return download_shape_files(self.geo, self.year)
