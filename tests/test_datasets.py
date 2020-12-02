import os

import pandas as pd
import pytest

from can_tools import scrapers, ALL_SCRAPERS

CONN_STR = os.environ.get("PG_CONN_STR", None)
if CONN_STR is not None:

    def clear_fkeys():
        import sqlalchemy as sa

        conn = sa.create_engine(CONN_STR)
        q = "alter table data.covid_official drop constraint if exists {};"
        cons = [
            "covid_official_provider_fkey",
            "covid_official_variable_id_fkey",
            "covid_official_demographic_id_fkey",
            "covid_official_location_id_fkey",
            # "covid_official_pkey",
        ]
        for c in cons:
            conn.execute(q.format(c))

    clear_fkeys()


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


@pytest.mark.parametrize("cls", ALL_SCRAPERS)
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


@pytest.mark.parametrize("cls", ALL_SCRAPERS)
def test_all_dataset_has_type(cls):
    assert hasattr(cls, "data_type")


@pytest.mark.parametrize("cls", ALL_SCRAPERS)
def test_covid_dataset_has_source(cls):
    if getattr(cls, "data_type", False) == "covid":
        assert hasattr(cls, "source")
        assert hasattr(cls, "state_fips") or getattr(cls, "has_location", False)
