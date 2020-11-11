import os

from can_tools import scrapers
import pytest
import pandas as pd


nodates = scrapers.DatasetBaseNoDate.__subclasses__()
yesdates = scrapers.DatasetBaseNeedsDate.__subclasses__()
all_ds = nodates + yesdates

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
        assert "county" in cols


def _test_data_structure(cls, df):
    if getattr(cls, "data_type", None) == "covid":
        _covid_dataset_tests(cls, df)


@pytest.mark.parametrize("cls", nodates)
def test_no_date_datasets(cls):
    d = cls()
    out = d.get()
    assert isinstance(out, pd.DataFrame)
    assert out.shape[0] > 0
    _test_data_structure(d, out)

    if CONN_STR is not None:
        d.put(CONN_STR, out)
        assert True


@pytest.mark.parametrize("cls", yesdates)
def test_need_date_datasets(cls):
    d = cls()
    out = d.get("2020-05-25")
    assert isinstance(out, pd.DataFrame)
    assert out.shape[0] > 0
    _test_data_structure(d, out)


@pytest.mark.parametrize("cls", all_ds)
def test_all_dataset_has_type(cls):
    assert hasattr(cls, "data_type")


@pytest.mark.parametrize("cls", all_ds)
def test_covid_dataset_has_source(cls):
    if getattr(cls, "data_type", False) == "covid":
        assert hasattr(cls, "source")
        assert hasattr(cls, "state_fips") or getattr(cls, "has_location", False)
