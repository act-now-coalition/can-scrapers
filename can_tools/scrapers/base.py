import io
import os
import pickle
import random

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, Optional, List

import pandas as pd
import sqlalchemy as sa

from can_tools.scrapers.db_util import TempTable


class CMU:
    def __init__(
        self,
        category="cases",
        measurement="cumulative",
        unit="people",
        age="all",
        race="all",
        sex="all",
    ):
        self.category = category
        self.measurement = measurement
        self.unit = unit
        self.age = age
        self.race = race
        self.sex = sex


def build_on_conflict_do_nothing_query(
    columns: List[str],
    dest_table: str,
    temp_table: str,
    unique_constraint: str,
    dest_schema: str = "data",
) -> str:
    """
    Construct sql query to insert data originally from `df` into `dest_schema.dest_table` via the
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
    pk : str
    table_name: str

    def __init__(self, execution_dt: pd.Timestamp):
        # Set execution date information
        self.execution_dt = execution_dt

        # Set storage path
        if "DATAPATH" in os.environ.keys():
            self.base_path = Path(os.environ["DATAPATH"])
        else:
            self.base_path = os.path.join(Path.home(), ".can-data")

        # Make sure the storage path exists and create if not
        if not self.base_path.exists():
            self.base_path.mkdir(parents=True)

    def _retrieve_dt(self, tz: str = "US/Eastern") -> pd.Timestamp:
        """Get the current datetime in a specific timezone"""
        out = pd.Timestamp.utcnow().tz_convert(tz).normalize().tz_localize(None)

        return out

    def _retrieve_vintage(self) -> pd.Timestamp:
        """Get the current UTC timestamp, at hourly resolution. Used as "vintage" in db"""
        return pd.Timestamp.utcnow().floor("h")

    def extract_CMU(
        self,
        df: pd.DataFrame,
        cmu: Dict[str, CMU],
        columns: List[str] = ["category", "measurement", "unit", "age", "race", "sex"],
        var_name: str = "variable",
    ) -> pd.DataFrame:
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

    def _filepath(self, raw: bool) -> str:
        """
        Method for determining the file path/file name -- Everything is
        stored using the following conventions:

        * `{rc}` is either `"raw"` or `"clean"` based on whether the
          data being read in is raw or clean data
        * `{classname}` comes from the class name
        * `{execution_dt}` is the execution datetime
        * `{ft}` is the file type

        The data will then be stored  at the path:

        `{rc}/{classname}/{execution_dt}.{ft}``

        Parameters
        ----------
        ft : str
            The filetype that the data should be stored as
        raw : bool
            Takes the value `True` if we are storing raw
            data

        Returns
        -------
        fp : str
            The autogenerated name for where the data will
            be stored
        """
        # Set file path pieces
        rc, ft = ("raw", "pickle") if raw else ("clean", "parquet")
        cn = self.__class__.__name__
        ed = self.execution_dt.strftime("%Y-%m-%d_%H")

        # Set filepath using its components
        fp = os.path.join(self.base_path, rc, cn, f"{ed}.{ft}")

        return fp

    def _read_clean(self) -> pd.DataFrame:
        """
        The `_read_clean` method reads a parquet file from the
        corresponding filepath

        Returns
        -------
        df : pd.DataFrame
            The data as a DataFrame
        """
        # Get the file path of the data
        fp = self._filepath(raw=False)
        if not os.path.exists(fp):
            msg = "The data that you are trying to read does not exist"
            raise ValueError(msg)

        # Read data
        df = pd.read_parquet(fp)

        return df

    def _read_raw(self) -> Any:
        """
        The `_read_raw` method reads raw data from a filepath

        Returns
        -------
        data :
            The data file as a string
        """
        # Get the file path of the data
        fp = self._filepath(raw=True)
        if not os.path.exists(fp):
            msg = "The data that you are trying to read does not exist"
            raise ValueError(msg)

        # Read the data
        with open(fp, "rb") as f:
            data = pickle.load(f)

        return data

    def _store_clean(self, df: pd.DataFrame) -> bool:
        """
        The `_store_clean` method saves the data into a parquet file
        at its corresponding filepath

        Parameters
        ----------
        df : pd.DataFrame
            The data stored in a DataFrame

        Returns
        -------
        success : bool
        """
        fp = self._filepath("parquet", raw=False)

        df.to_parquet(fp)

        return True

    def _store_raw(self, data: Any) -> bool:
        """
        The `_store_raw` method saves the data into its corresponding
        filepath

        Parameters
        ----------
        data :
            The data in its raw format

        Returns
        -------
        success : bool
            Whether the data was successfully stored at the filepath
        """
        # Get filepath
        fp = self._filepath(raw=True)

        with open(fp, "wb") as f:
            pickle.dump(data, f)

        return True

    def _insert_query(
        self, df: pd.DataFrame, table_name: str, temp_name: str, pk: str
    ) -> None:
        """
        Construct query for copying data from temporary table into the table

        By default it is build_conflict_do_nothing_query, but can be overridden by
        classes using this Mixin

        Parameters
        ----------
        df, table_name, temp_name, pk
            See build_on_conflict_do_nothing_query

        Returns
        -------
        _ : str
            The SQL query to be executed for insert

        """

        return build_on_conflict_do_nothing_query(df, table_name, temp_name, pk)

    @abstractmethod
    def fetch(self) -> Any:
        """
        The `fetch` method should retrieve the data in its raw form
        and return the data and the filetype that it should be
        saved as

        Returns
        -------
        data : Any
            The data in its raw format
        """
        pass

    def _fetch(self):
        """
        Fetches the raw data in whatever format it is originally stored
        in and dumps the raw data into storage using the `_store`
        method. If the download does not come in a text file format
        then we store it as a csv

        Returns
        -------
        success: bool
            Takes the value `True` if it successfully downloads and
            stores the data
        """
        data = self.fetch()
        success = self._store_raw(data)

        return success

    @abstractmethod
    def normalize(self, data: str) -> pd.DataFrame:
        """
        The `normalize` method should take the data in its raw form
        (as a string) and then clean the data

        Parameters
        ----------
        data : Any
            The raw data

        Returns
        -------
        df : pd.DataFrame
            The cleaned data as a DataFrame
        """
        pass

    def _normalize(self):
        """
        The `_normalize` method should take the data in its raw form
        from the storage bucket, clean the data, and then save the
        cleaned data into the bucket

        Returns
        -------
        success : bool
            A boolean indicating whether the data was successfully
            cleaned
        """
        # Ingest the data
        data = self._read_raw()

        # Clean data using `_normalize`
        df = self._normalize(data)
        success = self._store_clean(df)

        return success

    @abstractmethod
    def validate(self, df, df_hist):
        """
        The `validate` method checks what the tentative clean data looks
        like and then checks whether the updates are sensible -- If they
        are then it returns True otherwise it returns False and stops
        the fetch->normalize->validate->put DAG

        Parameters
        ----------
        df : pd.DataFrame
            The data that is being proposed for put
        df_hist : pd.DataFrame
            Historical data

        Returns
        -------
        validated : bool
            Whether we have validated the data
        """
        pass

    def _validate(self):
        """
        The `_validate` method loads the appropriate data and then
        checks the data using `validate` and determines whether the
        data has been validated

        Returns
        -------
        validated : bool
            Whether we have validated the data
        """
        # Load cleaned data
        df = self._read_clean()
        df_hist = None

        # Validate data
        validated = self.validate(df, df_hist)

        return validated

    def put(self, connstr: str) -> None:
        """
        Read DataFrame `df` from storage and put into corresponding
        PostgreSQL database

        Parameters
        ----------
        connstr : str
            String containing connection URI for connecting to postgres database

        Returns
        -------
        None

        """
        # Load cleaned data
        df = self._read_clean()

        if not hasattr(self, "pk"):
            msg = "field `pk` must be set for insertion"
            raise ValueError(msg)

        self._put(connstr, df, self.table_name, self.pk)

    def _put(self, connstr: str, df: pd.DataFrame, table_name: str, pk: str) -> None:
        "Internal _put method for dumping data using TempTable class"
        temp_name = "__" + table_name + str(random.randint(1000, 9999))

        with sa.create_engine(connstr).connect() as conn:
            kw = dict(temp=False, if_exists="replace", destroy=True)

            with TempTable(df, temp_name, conn, **kw):
                sql = self._insert_query(df, table_name, temp_name, pk)
                res = conn.execute(sql)
                print(f"Inserted {res.rowcount} rows into {table_name}")
