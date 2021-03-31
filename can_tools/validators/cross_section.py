from typing import List, Optional, Tuple
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


def _elementwise_comp_by_cat(
    method,
    df: pd.DataFrame,
    cat1: str,
    cat2: str,
    drop_levels: Optional[List[str]] = None,
) -> Tuple[bool, pd.MultiIndex]:
    df1, df2 = prepare_comparison(df, ["category"], [cat1], [cat2])
    if drop_levels is not None:
        df1.reset_index(level=drop_levels, inplace=True, drop=True)
        df2.reset_index(level=drop_levels, inplace=True, drop=True)

    # find common subset
    common_index = df1.index.intersection(df2.index)

    bools = method(df1.loc[common_index], df2.loc[common_index])["value"]
    if bools.all():
        return True, None
    else:
        return False, df1.index[~bools]


def cat1_ge_cat2(
    df: pd.DataFrame, cat1: str, cat2: str, drop_levels: Optional[List[str]] = None
) -> Tuple[bool, pd.MultiIndex]:
    """
    Checks that values from subset where `category == cat1` are
    greater than or equal to the values from subset where `category == cat2`
    """
    return _elementwise_comp_by_cat(pd.DataFrame.ge, df, cat1, cat2, drop_levels)


def cat1_gt_cat2(
    df: pd.DataFrame, cat1: str, cat2: str, drop_levels: Optional[List[str]] = None
) -> Tuple[bool, pd.MultiIndex]:
    """
    Checks that values from subset where `category == cat1` are
    greater than the values from subset where `category == cat2`
    """
    return _elementwise_comp_by_cat(pd.DataFrame.gt, df, cat1, cat2, drop_levels)
