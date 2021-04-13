from typing import Optional
import pandas as pd
import sqlalchemy as sa


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
