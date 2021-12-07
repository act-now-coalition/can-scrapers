from typing import Optional
import pandas as pd
import sqlalchemy as sa
from sqlalchemy.engine.base import Engine
from can_tools.models import Location, create_dev_engine


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


def find_unknown_variable_id(df: pd.DataFrame, engine: Engine = None, csv_rows=False):
    """Find any CMU variables in the specified dataframe that do not match an entry in the covid_variables file"""
    if not engine:
        engine = create_dev_engine()[0]
    variables = pd.read_sql("select * from covid_variables", engine)
    merged = df.merge(variables, on=["category", "measurement", "unit"], how="left")
    bad = merged["id"].isna()

    df_bad = df.loc[bad, :]
    if not csv_rows:
        return df_bad

    df_bad_combinations = df_bad[["category", "measurement", "unit"]].drop_duplicates()
    return df_bad_combinations.to_csv(index=False, header=False)


def find_unknown_location_id(df: pd.DataFrame, state_fips: int, engine: Engine = None, csv_rows=False):
    """Find any locations in the specified dataframe that do not match an entry in the locations file"""
    if not engine:
        engine = create_dev_engine()[0]
    locs = pd.read_sql("select * from locations", engine)
    if "location" in df.columns:
        good_rows = df.location.isin(locs.location)
    else:
        good_rows = df.location_name.isin(
            locs.loc[locs.state_fips == state_fips, :].name
        )
    bad_df = df.loc[~good_rows, :]
    
    if not csv_rows:
        return bad_df
    if "location" in df.columns:
        return bad_df["location"].drop_duplicates().to_csv(index=False, header=False)
    return bad_df["location_name"].drop_duplicates().to_csv(index=False, header=False)


def find_unknown_demographic_id(
    df: pd.DataFrame, engine: Engine = None, csv_rows=False
):
    """Find any demographic pairs in the specified dataframe that do not match an entry in the covid_demographics file"""
    if not engine:
        engine = create_dev_engine()[0]
    dems = pd.read_sql("select * from covid_demographics", engine)
    merged = df.merge(dems, on=["sex", "age", "race", "ethnicity"], how="left")
    bad = list(merged["id"].isna())

    df_bad = df.loc[bad, :]
    if not csv_rows:
        return df_bad

    df_bad_combinations = df_bad[["age", "race", "ethnicity", "sex"]].drop_duplicates()
    return df_bad_combinations.to_csv(index=False, header=False)


def find_duplicated_variable_entries(df: pd.DataFrame, has_location=False):
    """find duplicated variables in a scraper output"""
    df = df.reset_index(drop=True)
    location_col = "location" if has_location else "location_name"
    df_insert_keys = [
        "dt",
        "category",
        "measurement",
        "unit",
        "age",
        "race",
        "ethnicity",
        "sex",
        location_col,
    ]

    df_duplicated_idx = df.loc[:, df_insert_keys].duplicated(keep=False)
    return df.loc[df_duplicated_idx == True]
