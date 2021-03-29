import pandas as pd

from can_tools.validators.utils import list_index_cols


def values_increasing_over_time(df: pd.DataFrame) -> bool:
    """[summary]

    Args:
        df (pd.DataFrame): [description]

    Returns:
        bool: [description]
    """
    index_cols = list_index_cols(df)
    idf = df.set_index(index_cols)

    # remove dt and measurement
    index_cols.remove("dt")
    index_cols.remove("measurement")  # measurement always 'cumulative'
    sub = idf.query("measurement == 'cumulative'")["value"].unstack(level=index_cols)

    # check all remaining time series to make sure none is decreasing over time
    for col in sub.columns:
        series = sub[col].dropna().sort_index(level="dt")
        if (series.diff() < 0).any():
            return False

    return True
