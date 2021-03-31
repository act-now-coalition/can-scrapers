import pandas as pd

from can_tools.validators.utils import list_index_cols


class CumulativeValueDecreases(Exception):
    def __init__(self, info):
        self.info = info


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

    bad = {}

    # check all remaining time series to make sure none is decreasing over time
    for col in sub.columns:
        series = sub[col].dropna().sort_index(level="dt")
        series21 = series.loc["2021-01-01":, :]
        has_decrease = series21.diff() < 0
        if has_decrease.any():
            # find bad dates
            bad[col] = series21.loc[has_decrease].index.get_level_values("dt")

    if len(bad) > 0:
        return False, bad

    return True, None
