import pandas as pd

from can_tools.utils import determine_location_column


def prepare_selector_query(column_names, elements):
    "Takes a list of columns/values and only keeps values where column_names[i]==elements[i]"
    return " & ".join([f"{x} == '{y}'" for x, y in zip(column_names, elements)])


def prepare_comparison(df, column_names, elements1, elements2):
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

    loc_col = determine_location_column(df)

    # Index columns
    idx_cols = [
        "dt",
        loc_col,
        "category",
        "measurement",
        "unit",
        "age",
        "sex",
        "race",
        "ethnicity"
    ]
    for c in column_names:
        idx_cols.remove(c)

    # Subset the data for relevant subsets
    df1 = df.query(
        prepare_selector_query(column_names, elements1)
    ).set_index(
        idx_cols
    )[["value"]]
    df2 = df.query(
        prepare_selector_query(column_names, elements2)
    ).set_index(
        idx_cols
    )[["value"]]

    return df1, df2


def cat1_ge_cat2(df, cat1, cat2):
    """
    Checks that values from subset where `category == cat1` are
    greater than or equal to the values from subset where `category == cat2`
    """
    df1, df2 = prepare_comparison(df, ["category"], [cat1], [cat2])

    return df1.ge(df2)["value"].all()


def cat1_gt_cat2(df, cat1, cat2):
    """
    Checks that values from subset where `category == cat1` are
    greater than the values from subset where `category == cat2`
    """
    df1, df2 = prepare_comparison(df, ["category"], [cat1], [cat2])

    return df1.gt(df2)["value"].all()


def cat1_le_cat2(df, cat1, cat2):
    """
    Checks that values from subset where `category == cat1` are
    less than or equal to the values from subset where `category == cat2`
    """
    df1, df2 = prepare_comparison(df, ["category"], [cat1], [cat2])

    return df1.le(df2)["value"].all()


def cat1_lt_cat2(df, cat1, cat2):
    """
    Checks that values from subset where `category == cat1` are
    less than the values from subset where `category == cat2`
    """
    df1, df2 = prepare_comparison(df, ["category"], [cat1], [cat2])

    return df1.lt(df2)["value"].all()
