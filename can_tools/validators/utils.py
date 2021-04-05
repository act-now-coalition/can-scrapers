from typing import List
import pandas as pd

from can_tools.utils import determine_location_column


def prepare_selector_query(column_names: List[str], elements: List[str]) -> str:
    "Takes a list of columns/values and only keeps values where column_names[i]==elements[i]"
    return " & ".join([f"{x} == '{y}'" for x, y in zip(column_names, elements)])


def list_index_cols(df: pd.DataFrame) -> List[str]:
    """List the columns that form the index for a normalize DataFrame

    Args:
        df (pd.DataFrame): A normalized DataFrame

    Returns:
        List[str]: A list of column names for the index
    """
    loc_col = determine_location_column(df)

    # Index columns
    return [
        "dt",
        loc_col,
        "category",
        "measurement",
        "unit",
        "age",
        "sex",
        "race",
        "ethnicity",
    ]


def prepare_indexed_df(df: pd.DataFrame) -> pd.DataFrame:
    """Set index on a normalized df

    Args:
        df (pd.DataFrame): DataFrame before setting index

    Returns:
        pd.DataFrame: DataFrame after setting index
    """
    return df.set_index(list_index_cols(df))
