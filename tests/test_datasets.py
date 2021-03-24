from can_tools.scrapers.official.federal.CDC.cdc_coviddatatracker import (
    CDCCovidDataTracker,
)
import os

import pandas as pd
import pytest
import sqlalchemy as sa

from can_tools import ALL_SCRAPERS
from can_tools.models import Base, create_dev_engine

CONN_STR = os.environ.get("CAN_PG_CONN_STR", None)
VERBOSE = bool(os.environ.get("CAN_TESTS_VERBOSE", False))
if CONN_STR is not None:
    engine = sa.create_engine(CONN_STR, echo=VERBOSE)
    Base.metadata.reflect(bind=engine)
    Base.metadata.create_all(bind=engine)
else:
    engine, sess = create_dev_engine(verbose=VERBOSE)


def _covid_dataset_tests(cls, df):
    want_cols = [
        "vintage",
        "dt",
        "category",
        "measurement",
        "unit",
        "age",
        "race",
        "ethnicity",
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


@pytest.mark.parametrize("cls", ALL_SCRAPERS)
def test_datasets(cls):
    execution_date = (pd.Timestamp.today() - pd.Timedelta("1 days")).strftime(
        "%Y-%m-%d"
    )
    if cls == CDCCovidDataTracker:
        d = cls(execution_date, state="CA")
    else:
        d = cls(execution_date)

    raw = d.fetch()

    assert raw is not None
    clean = d.normalize(raw)
    assert isinstance(clean, pd.DataFrame)
    assert clean.shape[0] > 0
    _test_data_structure(d, clean)

    d.put(engine, clean)


@pytest.mark.parametrize("cls", ALL_SCRAPERS)
def test_all_dataset_has_type(cls):
    assert hasattr(cls, "data_type")


@pytest.mark.parametrize("cls", ALL_SCRAPERS)
def test_all_dataset_has_location_type(cls):
    assert hasattr(cls, "location_type")


@pytest.mark.parametrize("cls", ALL_SCRAPERS)
def test_covid_dataset_has_source(cls):
    if getattr(cls, "data_type", False) == "covid":
        assert hasattr(cls, "source")
        assert hasattr(cls, "state_fips") or getattr(cls, "has_location", False)


@pytest.mark.parametrize("cls", ALL_SCRAPERS)
def test_all_datasets_has_source_name(cls):
    assert hasattr(cls, "source_name")
