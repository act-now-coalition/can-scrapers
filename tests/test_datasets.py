import os

import pandas as pd
import pytest

from can_tools import scrapers

all_ds = scrapers.DatasetBase.__subclasses__()

CONN_STR = os.environ.get("PG_CONN_STR", None)


def _covid_dataset_tests(cls, df):
    want_cols = [
        "vintage",
        "dt",
        "category",
        "measurement",
        "unit",
        "age",
        "race",
        "sex",
        "value",
    ]
    cols = list(df)
    assert all(c in cols for c in want_cols)

    has_location = getattr(cls, "has_location", None)
    if has_location is None:
        return
    if has_location:
        assert "location" in cols
    else:
        assert "location_name" in cols


def _test_data_structure(cls, df):
    if getattr(cls, "data_type", None) == "covid":
        _covid_dataset_tests(cls, df)


@pytest.mark.parametrize("cls", all_ds)
def test_datasets(cls):
    execution_date = pd.to_datetime("2020-11-10")
    d = cls(execution_date)
    raw = d.fetch()
    assert raw is not None
    clean = d.normalize(raw)
    assert isinstance(clean, pd.DataFrame)
    assert clean.shape[0] > 0
    _test_data_structure(d, clean)

    if CONN_STR is not None:
        d.put(CONN_STR, clean)
        assert True

@pytest.mark.parametrize("cls", all_ds)
def test_all_dataset_has_type(cls):
    assert hasattr(cls, "data_type")


@pytest.mark.parametrize("cls", all_ds)
def test_covid_dataset_has_source(cls):
    if getattr(cls, "data_type", False) == "covid":
        assert hasattr(cls, "source")
        assert hasattr(cls, "state_fips") or getattr(cls, "has_location", False)
