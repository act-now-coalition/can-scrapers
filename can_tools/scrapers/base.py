import random
from abc import ABC, abstractmethod
from collections import namedtuple
from typing import List, Dict

import pandas as pd
import sqlalchemy as sa

from can_tools.scrapers.db_util import TempTable

CMU = namedtuple(
    "CMU",
    ["category", "measurement", "unit", "age", "race", "sex"],
    defaults=["cases", "cumulative", "people", "all", "all", "all"],
)


class DatasetBase(ABC):
    """
    Attributes
    ----------
    autodag: bool = True
        Whether an airflow dag should be automatically generated for this class

    data_type: str = "general"
        The type of data for this scraper. This is often set to "covid"
        by subclasses

    table_name: str
        The name of the database table where this data should be inserted
    """

    autodag: bool = True
    data_type: str = "general"
    table_name: str

    def __init__(self):
        pass

    def extract_CMU(
        self,
        df: pd.DataFrame,
        cmu: Dict[str, CMU],
        columns: List[str] = ["category", "measurement", "unit", "age", "race", "sex"],
        var_name: str = "variable",
    ):
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
        columns: List[str]
            A list of columns to set using the `cmu` dict
        var_name: str
            The name of the column in `df` that should be used to lookup
            items from the `cmu` dict for unpacking columns

        Returns
        -------
        df : pd.DataFrame
            A copy of the DataFrame passed in that has new columns
            "category", "measurement", and "unit"
        """
        foo = df[var_name]
        return df.assign(
            **{c: foo.map(lambda x: cmu[x].__getattribute__(c)) for c in columns}
        )

    def put(self, connstr: str, df: pd.DataFrame):
        """
        Put the DataFrame into the Postgres database

        Parameters
        ----------
        connstr : str
            The sqlalchemy connection URI to be used to connect to the database
        df : pd.DataFrame
            The DataFrame to insert

        Notes
        -----
        Must be implemented by subclass

        """
        pass

    def _retrieve_dt(self, tz: str = "US/Eastern") -> pd.Timestamp:
        """Get the current datetime in a specific timezone"""
        out = pd.Timestamp.utcnow().tz_convert(tz).normalize().tz_localize(None)

        return out

    def _retrieve_vintage(self) -> pd.Timestamp:
        """Get the current UTC timestamp, at hourly resolution. Used as "vintage" in db"""
        return pd.Timestamp.utcnow().floor("h")


class DatasetBaseNoDate(DatasetBase, ABC):
    """
    Base class for a dataset that collects data "as is" from a specified resource
    rather than requesting data for a specific date
    """

    get_needs_date: bool = False

    @abstractmethod
    def get(self):
        """
        Get data and return as DataFrame formatted for insertion into the database

        TODO: describe columns and types...

        Returns
        -------
        df: pd.DataFrame
            The current dataset for the underlying resource

        """
        raise NotImplementedError("Must be implemented by subclass")


class DatasetBaseNeedsDate(DatasetBase, ABC):
    """
    Base class for a dataset that collects data from a resource for a specific date
    """

    get_needs_date: bool = True

    @abstractmethod
    def get(self, date: str):
        """
        Get data for specific date and return as DataFrame ready to be inserted into the database

        TODO: describe columns and types...

        Parameters
        ----------
        date : str
            A string representing the date for which to obtain data

        Returns
        -------
         df: pd.DataFrame
            The dataset for `date` from the underlying resource

        """
        raise NotImplementedError("Must be implemented by subclass")

    def transform_date(self, date: pd.Timestamp) -> pd.Timestamp:
        """
        Transform a given date to a date that can be used to interact with remote service

        Parameters
        ----------
        date : pd.Timestamp
            The date to be transformed

        Returns
        -------

        """
        return date

    def quit_early(self, date: pd.Timestamp) -> bool:
        """
        Determine whether the scraper should quit early based on the date

        Used by scrapers when data is not available from the underlying resource
        for a specific date.

        Parameters
        ----------
        date : pd.Timestamp
            The date for requested data

        Returns
        -------
        should_stop: bool
            A boolean dictating if the request to fetch data should be stopped before
            network requests are initiated

        """
        return False


def build_on_conflict_do_nothing_query(
    columns: List[str],
    dest_table: str,
    temp_table: str,
    unique_constraint: str,
    dest_schema: str = "data",
) -> str:
    """
    Construct sql query to insert data originally from `df` into `s_home.t_home` via the
    temporary database table `temp_table`.

    If there are any conflicts on the unique index `pk`, do nothing

    Parameters
    ----------
    columns: List[str]
        A list of column names found in the temporary table that should be inserted into the final table
    dest_table : str
        The destination table
    temp_table : str
        The temporary table name (in public schema)
    unique_constraint : str
        A string referencing the unique key for the destination table
    dest_schema : str
        The name of the postgres schema for the destination

    Returns
    -------
    query: str
        The SQL query for copying data from the tempmorary table to the destination one

    """
    colnames = ", ".join(columns)
    cols = "(" + colnames + ")"
    if not unique_constraint.startswith("("):
        unique_constraint = f"({unique_constraint})"

    return f"""
    INSERT INTO {dest_schema}.{dest_table} {cols}
    SELECT {colnames} from {temp_table}
    ON CONFLICT {unique_constraint} DO NOTHING;
    """


class InsertWithTempTableMixin:
    """
    Mixin providing ability to `put` a DataFrame into the database
    by first uploading the DataFrame to a temporary table within the database
    then issuing a query to `INSERT INTO ... SELECT...`

    This gives opportunity for the programmer to join in foreign keys from other tables
    when uploading data

    """

    pk: str
    table_name: str

    def _insert_query(
        self, df: pd.DataFrame, table_name: str, temp_name: str, pk: str
    ) -> None:
        """
        Construct query for copying data from temporary table into the official table

        By default it is build_conflict_do_nothing_query, but can be overridden by
        classes using this Mixin

        Parameters
        ----------
        df, table_name, temp_name, pk
            See build_on_conflict_do_nothing_query

        Returns
        -------
        None

        """

        return build_on_conflict_do_nothing_query(df, table_name, temp_name, pk)

    def _put(self, connstr: str, df: pd.DataFrame, table_name: str, pk: str) -> None:
        temp_name = "__" + table_name + str(random.randint(1000, 9999))

        with sa.create_engine(connstr).connect() as conn:
            kw = dict(temp=False, if_exists="replace", destroy=True)

            with TempTable(df, temp_name, conn, **kw):
                sql = self._insert_query(df, table_name, temp_name, pk)
                res = conn.execute(sql)
                print(f"Inserted {res.rowcount} rows into {table_name}")

    def put(self, connstr: str, df: pd.DataFrame) -> None:
        """
        Upload DataFrame `df` to table via a temp table + "insert into ... select" query

        Parameters
        ----------
        connstr : str
            String containing connection URI for connecting to postgres database
        df : pd.DataFrame
            pandas DataFrame containing data to be uploaded

        Returns
        -------

        """
        if df is None:
            if hasattr(self, "df"):
                df = self.df
            else:
                raise ValueError("No df found, please pass")

        if not hasattr(self, "pk"):
            msg = "field `pk` must be set on subclass of OnConflictNothingBase"
            raise ValueError(msg)

        self._put(connstr, df, self.table_name, self.pk)
