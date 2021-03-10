from can_tools.models import Base

import os
import pickle

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, List, Optional, Type

import pandas as pd
import us
from sqlalchemy.engine.base import Engine


# `us` v2.0 removed DC from the `us.STATES` list, so we are creating
# our own which includes DC. In v3.0, there will be an env option to
# include `DC` in the `us.STATES` list and, if we upgrade, we should
# activate that option and replace this with just `us.STATES`
ALL_STATES_PLUS_DC = us.STATES + [us.states.DC]


class CMU:
    def __init__(
        self,
        category="cases",
        measurement="cumulative",
        unit="people",
        age="all",
        race="all",
        ethnicity="all",
        sex="all",
    ):
        self.category = category
        self.measurement = measurement
        self.unit = unit
        self.age = age
        self.race = race
        self.ethnicity = ethnicity
        self.sex = sex


class DatasetBase(ABC):
    """
    Attributes
    ----------
    autodag: bool = True
        Whether an airflow dag should be automatically generated for this class

    data_type: str = "general"
        The type of data for this scraper. This is often set to "covid"
        by subclasses

    table: Type[Base]
        The SQLAlchemy base table where this data should be inserted

    location_type: Optional[str]
        Optional information used when a scraper only retrieves data about a
        single type of geography. It will set the `"location_type"` column
        to this value (when performing the `put`) if `"location_type"` is not
        already set in the df
    """

    autodag: bool = True
    data_type: str = "general"
    table: Type[Base]
    location_type: Optional[str]
    base_path: Path
    source: str

    def __init__(self, execution_dt: pd.Timestamp = pd.Timestamp.utcnow()):
        # Set execution date information
        self.execution_dt = pd.to_datetime(execution_dt, utc=True)

        # Set storage path
        if "DATAPATH" in os.environ.keys():
            self.base_path = Path(os.environ["DATAPATH"])
        else:
            self.base_path = Path.home() / ".can-data"

        # Make sure the storage path exists and create if not
        if not self.base_path.exists():
            self.base_path.mkdir(parents=True)

    def quit_early(self) -> bool:
        """
        Whether or not to bail on this scraper before starting

        Hook to allow subclasses to quit early based on self.execution_dt
        """

        return False

    def _retrieve_dt(self, tz: str = "US/Eastern") -> pd.Timestamp:
        """Get the current datetime in a specific timezone"""
        out = self.execution_dt.tz_convert(tz).normalize().tz_localize(None)

        return out

    def _retrieve_dtm1d(self, tz: str = "US/Eastern") -> pd.Timestamp:
        """Get the datetime of one day ago in a specific timezone """
        return self._retrieve_dt(tz) - pd.Timedelta(days=1)

    def _retrieve_vintage(self) -> pd.Timestamp:
        """Get the current UTC timestamp, at hourly resolution. Used as "vintage" in db"""
        return self.execution_dt.floor("h")

    def extract_CMU(
        self,
        df: pd.DataFrame,
        cmu: Dict[str, CMU],
        columns: List[str] = [
            "category",
            "measurement",
            "unit",
            "age",
            "race",
            "ethnicity",
            "sex",
        ],
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
        variable_column = df[var_name]
        out = df.copy()
        for col in columns:
            out[col] = variable_column.map(lambda x: cmu[x].__getattribute__(col))
        return out

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
        ed = self.execution_dt.strftime(r"%Y-%m-%d_%H")

        # Set filepath using its components
        fp = self.base_path / rc / cn
        if not fp.exists():
            fp.mkdir(parents=True)
        fn = fp / f"{ed}.{ft}"

        return fn

    def _read_clean(self) -> pd.DataFrame:
        """
        The `_read_clean` method reads a parquet file from the
        corresponding filepath

        Returns
        -------
        df : pd.DataFrame
            The data as a DataFrame
        """
        fp = self._filepath(raw=False)
        if not os.path.exists(fp):
            msg = "The data that you are trying to read does not exist"
            raise ValueError(msg)

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
        fp = self._filepath(raw=True)
        if not os.path.exists(fp):
            msg = "The data that you are trying to read does not exist"
            raise ValueError(msg)

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
        filename : string
            The path to the file where the data was written
        """
        filename = self._filepath(raw=False)
        df.to_parquet(filename)
        return filename

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
        filename : string
            The path to the file where the data was written
        """
        filename = self._filepath(raw=True)

        with open(filename, "wb") as f:
            pickle.dump(data, f)

        return filename

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
        filename : string
            The path to the file where the data was written
        """
        data = self.fetch()
        return self._store_raw(data)

    @abstractmethod
    def normalize(self, data: Any) -> pd.DataFrame:
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
        filename : string
            The path to the file where the data was written
        """
        # Ingest the data
        data = self._read_raw()

        # Clean data using `_normalize`
        df = self.normalize(data)
        return self._store_clean(df)

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
        return True

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
        return self.validate(df, df_hist)

    def put(self, engine: Engine, df: pd.DataFrame) -> bool:
        """
        Read DataFrame `df` from storage and put into corresponding
        PostgreSQL database

        Parameters
        ----------
        engine : sqlalchemy.engine.base.Engine
            A sqlalchemy engine

        Returns
        -------
        success : bool
            Did the insert succeed. Always True if function completes

        rows_inserted: int
            How many rows were inserted into the database

        rows_deleted: int
            How many rows were deleted from the temp table
        """
        return self._put_exec(engine, df)

    def _put(self, engine: Engine) -> None:
        """
        Read DataFrame `df` from storage and put into corresponding
        PostgreSQL database

        Parameters
        ----------
        engine : sqlalchemy.engine.base.Engine
            A sqlalchemy engine

        Returns
        -------
        success : bool
            Did the insert succeed. Always True if function completes

        rows_inserted: int
            How many rows were inserted into the database

        rows_deleted: int
            How many rows were deleted from the temp table
        """
        df = self._read_clean()
        return self.put(engine, df)

    @abstractmethod
    def _put_exec(self, engine: Engine, df: pd.DataFrame) -> None:
        "Internal _put method for dumping data using TempTable class"
        pass

    def find_unknown_variable_id(self, engine: Engine, df: pd.DataFrame):
        variables = pd.read_sql("select * from covid_variables", engine)
        merged = df.merge(variables, on=["category", "measurement", "unit"], how="left")
        bad = merged["id"].isna()
        return df.loc[bad, :]

    def find_unknown_location_id(self, engine: Engine, df: pd.DataFrame):
        locs = pd.read_sql("select * from locations", engine)
        good_rows = df.location_name.isin(
            locs.loc[locs.state_fips == self.state_fips, :].name
        )
        return df.loc[~good_rows, :]

    def fetch_normalize(self):
        "Call `self.normalize(self.fetch())`"
        data = self.fetch()
        return self.normalize(data)
