from typing import Type
from airflow import DAG
from can_tools.scrapers.base import DatasetBase, ALL_SCRAPERS
from common import (
    default_dag_kw,
    make_fetch_op,
    make_normalize_op,
    make_put_op,
    make_validate_op,
)


def _prep_kw(ix, cls, suffix):
    # randomize start time to not all dags run at exactly the same time
    letters = list("abcdefghijklmnopqrstuvwxyz")
    name_lower = cls.__name__.lower()
    hour = "1-23/4" if name_lower[0] < "m" else "0-22/4"
    minute = (ix * 2) % 59
    # if cls == DC:
    #     hour = "15-23/2"
    # if cls == USAFactsCases or cls == USAFactsDeaths:
    #     hour = "0-22/2"
    #     minute = 33

    sched = "{min} {hr} * * *".format(min=minute, hr=hour)

    return default_dag_kw(
        dag_id="{}_{}".format(cls.__name__, suffix),
        schedule_interval=sched,
    )


def make_needsdate_dag(ix, cls: Type[DatasetBase]):
    with DAG(**_prep_kw(ix, cls, "needs_date")) as dag:
        fetch = make_fetch_op(cls)
        normalize = make_normalize_op(cls)
        validate = make_validate_op(cls)
        put = make_put_op(cls)

        fetch >> normalize >> validate >> put
    return dag


for ix, cls in enumerate(ALL_SCRAPERS):
    if not cls.autodag:
        continue
    globals()["dag_{}".format(cls.__name__)] = make_needsdate_dag(ix, cls)
