from typing import Optional
import pandas as pd
import sqlalchemy as sa
from sqlalchemy.engine.base import Engine


def determine_location_column(df: pd.DataFrame) -> str:
    """Return name of column containing location identifier

    Args:
        df: Normalized DataFrame

    Raises:
        ValueError: raised when locaiton column could not be found.

    Returns:
       str : Column name contianing location identifier
    """

    options = ["location", "location_name"]
    for col in options:
        if col in df.columns:
            return col

    raise ValueError(f"Couldn't find location column. Searched for {options}")


def is_time_series(df: pd.DataFrame) -> bool:
    "Check if normalized DataFrame contains time series data"
    return df["dt"].nunique() > 1


def load_most_recent_cdc(
    source: str, engine: Optional[sa.engine.Engine]
) -> pd.DataFrame:
    """Loads most recent cdc data either from scraper output, sql, or parquet file

    Args:
        source: Where to get data. One of "scraper_output", "sql", "parquet"
        engine: sqlalchemy engine for sql queries. Only necessary when source = "sql"

    Returns:
        pd.DataFrame: DataFrame with CDC Covid Tracker data
    """
    pass


def find_unknown_variable_id(engine: Engine, df: pd.DataFrame):
    """Find any CMU variables in the specified dataframe that do not match an entry in the covid_variables file"""
    variables = pd.read_sql("select * from covid_variables", engine)
    merged = df.merge(variables, on=["category", "measurement", "unit"], how="left")
    bad = merged["id"].isna()
    return df.loc[bad, :]


def find_unknown_location_id(engine: Engine, df: pd.DataFrame, state_fips: int):
    """Find any locations in the specified dataframe that do not match an entry in the locations file"""
    locs = pd.read_sql("select * from locations", engine)
    good_rows = df.location_name.isin(locs.loc[locs.state_fips == state_fips, :].name)
    return df.loc[~good_rows, :]


def find_unknown_demographic_id(engine: Engine, df: pd.DataFrame):
    """Find any demographic pairs in the specified dataframe that do not match an entry in the covid_demographics file"""
    dems = pd.read_sql("select * from covid_demographics", engine)
    merged = df.merge(dems, on=["sex", "age", "race", "ethnicity"], how="left")
    bad = list(merged["id"].isna())
    return df.loc[bad, :]
