from datetime import datetime, timedelta
from urllib.error import HTTPError

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
import pandas as pd


def _maybe_log_source(c, connstr, df):
    state_fips = getattr(c, "state_fips", None)
    if getattr(c, "data_type", None) == "covid":
        src = getattr(c, "source", None)
        if src is not None:
            c.log_covid_source(connstr, df, src, state_fips)
            print("logged source to db")


def getput_no_date(cls, task_id="getput") -> PythonOperator:
    def inner(**kw):
        db = PostgresHook(postgres_conn_id="postgres_covid").get_uri()
        c = cls()
        print("About to fetch...")
        df = c.get()
        if isinstance(df, pd.DataFrame):
            print(f"Fetch df with shape {df.shape}")

        c.put(db, df)
        print("Uploaded to db")
        # _maybe_log_source(c, db, df)

    op = PythonOperator(python_callable=inner, task_id=task_id)

    return op


def getput_needs_date(cls, task_id="getput") -> PythonOperator:
    def inner(ds, **kw):
        c = cls()
        dt = c.transform_date(pd.to_datetime(ds))
        if c.quit_early(dt):
            print("Requested to quit early -- bailing")
            return

        print(f"dt: {dt} and ds: {ds}")

        db = PostgresHook(postgres_conn_id="postgres_covid").get_uri()

        print("About to fetch...")
        try:
            df = c.get(dt)
        except HTTPError as e:
            print("Got an http error, not marking as failed {}".format(e))
            return
        except Exception as e:
            raise e

        if isinstance(df, pd.DataFrame):
            print(f"Fetch df with shape {df.shape}")

        c.put(db, df)
        print("Uploaded to db")
        _maybe_log_source(c, db, df)

    op = PythonOperator(python_callable=inner, task_id=task_id, provide_context=True)

    return op


def _make_default_args(**kw):
    out = {
        "owner": "sglyon",
        "depends_on_past": False,
        "start_date": datetime(2020, 5, 18, 9, 30),
        "end_date": datetime(2021, 5, 5, 9, 30),
        "email": ["spencer.lyon@valorumdata.com"],
        "email_on_failure": True,
        "email_on_retry": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    }
    out.update(kw)
    return out


def default_dag_kw(dag_id, default_args=dict(), **kw):
    out = dict(
        dag_id=dag_id,
        default_args=_make_default_args(**default_args),
        schedule_interval="30 9 * * *",
        max_active_runs=4,
        catchup=False,
    )
    out.update(kw)
    return out
