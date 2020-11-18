from airflow import DAG
from can_tools.scrapers.base import DatasetBaseNoDate, DatasetBaseNeedsDate
from common import default_dag_kw, getput_no_date, getput_needs_date


def _prep_kw(ix, cls, suffix):
    # randomize start time to not all dags run at exactly the same time
    letters = list("abcdefghijklmnopqrstuvwxyz")
    name_lower = cls.__name__.lower()
    hour = "1-23/2" if name_lower[0] < "m" else "0-22/2"
    minute = (ix * 2) % 59
    # if cls == DC:
    #     hour = "15-23/2"
    # if cls == USAFactsCases or cls == USAFactsDeaths:
    #     hour = "0-22/2"
    #     minute = 33

    sched = "{min} {hr} * * *".format(min=minute, hr=hour)

    return default_dag_kw(
        dag_id="{}_{}".format(cls.__name__, suffix), schedule_interval=sched,
    )


def make_nodate_dag(ix, cls):
    with DAG(**_prep_kw(ix, cls, "nodate")) as dag:
        op = getput_no_date(cls, task_id="getput")
    return dag


for ix, cls in enumerate(DatasetBaseNoDate.__subclasses__()):
    if not cls.autodag:
        continue
    globals()["dag_{}".format(cls.__name__)] = make_nodate_dag(ix, cls)


def make_needsdate_dag(ix, cls):
    with DAG(**_prep_kw(ix, cls, "needs_date")) as dag:
        op = getput_needs_date(cls, task_id="getput")
    return dag


for ix, cls in enumerate(DatasetBaseNeedsDate.__subclasses__()):
    if not cls.autodag:
        continue
    globals()["dag_{}".format(cls.__name__)] = make_needsdate_dag(ix, cls)
