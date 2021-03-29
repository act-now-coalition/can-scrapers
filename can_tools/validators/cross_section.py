from typing import List, Tuple
import pandas as pd


from can_tools.validators.utils import prepare_indexed_df, prepare_selector_query


def prepare_comparison(
    df: pd.DataFrame,
    column_names: List[str],
    elements1: List[str],
    elements2: List[str],
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Preparatory step for doing column-wise comparisons

    Parameters
    ----------
    df : pd.DataFrame
        The output of a `normalize` method
    column_names : list
        The list of column names that should be selected on
    elements1 : list
        The list of values that each column name should take
        for the first set of values
    elements2 : list
        The list of values that each column name should take
        for the second set of values

    Returns
    -------
    df1, df2 : pd.DataFrame
        Two dataframes that have been subsetted to allow for comparison
    """

    idf = prepare_indexed_df(df)

    # Subset the data for relevant subsets
    df1 = idf.query(prepare_selector_query(column_names, elements1))[["value"]]
    df2 = idf.query(prepare_selector_query(column_names, elements2))[["value"]]

    return df1, df2


def cat1_ge_cat2(df: pd.DataFrame, cat1: str, cat2: str) -> bool:
    """
    Checks that values from subset where `category == cat1` are
    greater than or equal to the values from subset where `category == cat2`
    """
    df1, df2 = prepare_comparison(df, ["category"], [cat1], [cat2])

    return df1.ge(df2)["value"].all()


def cat1_gt_cat2(df: pd.DataFrame, cat1: str, cat2: str) -> bool:
    """
    Checks that values from subset where `category == cat1` are
    greater than the values from subset where `category == cat2`
    """
    df1, df2 = prepare_comparison(df, ["category"], [cat1], [cat2])

    return df1.gt(df2)["value"].all()


def cat1_le_cat2(df: pd.DataFrame, cat1: str, cat2: str) -> bool:
    """
    Checks that values from subset where `category == cat1` are
    less than or equal to the values from subset where `category == cat2`
    """
    df1, df2 = prepare_comparison(df, ["category"], [cat1], [cat2])

    return df1.le(df2)["value"].all()


def cat1_lt_cat2(df: pd.DataFrame, cat1: str, cat2: str) -> bool:
    """
    Checks that values from subset where `category == cat1` are
    less than the values from subset where `category == cat2`
    """
    df1, df2 = prepare_comparison(df, ["category"], [cat1], [cat2])

    return df1.lt(df2)["value"].all()
