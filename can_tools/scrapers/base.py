import random
from abc import ABC, abstractmethod
from collections import namedtuple
from typing import Optional

import pandas as pd
import sqlalchemy as sa

from .db_util import TempTable


CMU = namedtuple(
    "CMU",
    ["category", "measurement", "unit", "age", "race", "sex"],
    defaults=["cases", "cumulative", "people", "all", "all", "all"]
)


class DatasetBase(ABC):
    autodag: bool = True
    data_type: str = "general"
    table_name: str

    def __init__(self):
        pass

    def extract_cat_measurement_unit(self, df, cmu):
        """
        Adds columns "category", "measurement", and "unit" to df

        Parameters
        ----------
        df : pd.DataFrame
            This DataFrame must have the column `variable` and the
            unique elements of `variable` must be keys in the
            `cmu_dict`
        cmu : dict(str -> CMU)
            This dictionary maps variable names into a subcategory,
            measurement, and unit using a CMU namedtuple

        Returns
        -------
        df : pd.DataFrame
            A copy of the DataFrame passed in that has new columns
            "category", "measurement", and "unit"
        """
        return df.assign(
            category=df["variable"].map(lambda x: cmu[x].category),
            measurement=df["variable"].map(lambda x: cmu[x].measurement),
            unit=df["variable"].map(lambda x: cmu[x].unit),
            age=df["variable"].map(lambda x: cmu[x].age),
            race=df["variable"].map(lambda x: cmu[x].race),
            sex=df["variable"].map(lambda x: cmu[x].sex),
        )

    @abstractmethod
    def put(self, conn, df):
        pass

    def _retrieve_dt(self, tz="US/Eastern"):
        out = pd.Timestamp.utcnow().tz_convert(tz).normalize().tz_localize(None)

        return out

    def _retrieve_vintage(self):
        return pd.Timestamp.utcnow().floor("h")

        pass


class DatasetBaseNoDate(DatasetBase, ABC):
    get_needs_date = False

    @abstractmethod
    def get(self):
        raise NotImplementedError("Must be implemented by subclass")


class DatasetBaseNeedsDate(DatasetBase, ABC):
    get_needs_date = True

    @abstractmethod
    def get(self, date: str):
        raise NotImplementedError("Must be implemented by subclass")

    def transform_date(self, date: pd.Timestamp) -> pd.Timestamp:
        return date

    def quit_early(self, date: pd.Timestamp) -> bool:
        return False


def _build_on_conflict_do_nothing_query(
        df: pd.DataFrame, t_home: str, t_temp: str, pk: str
    ):
    colnames = ", ".join(list(df))
    cols = "(" + colnames + ")"
    if not pk.startswith("("):
        pk = f"({pk})"

    return f"""
    INSERT INTO data.{t_home} {cols}
    SELECT {colnames} from {t_temp}
    ON CONFLICT {pk} DO NOTHING;
    """


class InsertWithTempTable(DatasetBase, ABC):
    pk: str

    def _insert_query(
            self, df: pd.DataFrame, table_name: str, temp_name: str, pk: str
        ):

        out = _build_on_conflict_do_nothing_query(df, table_name, temp_name, pk)
        return out

    def _put(self, connstr: str, df: pd.DataFrame, table_name: str, pk: str):
        temp_name = "__" + table_name + str(random.randint(1000, 9999))

        with sa.create_engine(connstr).connect() as conn:
            kw = dict(temp=False, if_exists="replace", destroy=True)

            with TempTable(df, temp_name, conn, **kw):
                sql = self._insert_query(df, table_name, temp_name, pk)
                conn.execute(sql)

    def put(self, connstr: str, df=None):
        if df is None:
            if hasattr(self, "df"):
                df = self.df
            else:
                raise ValueError("No df found, please pass")

        if not hasattr(self, "pk"):
            msg = "field `pk` must be set on subclass of OnConflictNothingBase"
            raise ValueError(msg)

        self._put(connstr, df, self.table_name, self.pk)
