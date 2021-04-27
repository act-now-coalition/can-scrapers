import pprint
from can_tools.validators import cross_section, timeseries
from can_tools import utils
from typing import Any, Dict, List, Optional, Type

import os
import pickle
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, List, Optional, Type

import pandas as pd
import us
from sqlalchemy.engine.base import Engine

from can_tools.models import Base

# `us` v2.0 removed DC from the `us.STATES` list, so we are creating
# our own which includes DC. In v3.0, there will be an env option to
# include `DC` in the `us.STATES` list and, if we upgrade, we should
# activate that option and replace this with just `us.STATES`
ALL_STATES_PLUS_DC = us.STATES + [us.states.DC]
ALL_STATES_PLUS_TERRITORIES = us.states.STATES_AND_TERRITORIES + [us.states.DC]


class CMU:
    """Define variable and demographic dimensions for an observation

    Variable dimensions include:

    - category: The 'type' of variable. Examples are ``cases``, ``total_vaccine_completed``
    - measurement: The form of measurement, e.g. ``cumulative``, ``new``
    - unit: The unit of measurement, e.g. ``people``, ``doses``

    Demographic dimensions include:

    - age: the age group, e.g. ``1-10``, ``40-49``, ``65_plus``
    - race: the race, e.g. ``white``, ``black``
    - ethnicity: the ethnicity, e.g. ``hispanic``, ``non-hispanic``
    - sex: the sex, ``male``, ``female``, ``uknown``

    .. note::

        All demographic dimensions allow a value of ``all``, which is interpreted
        as the observation corresponding to all groups of that dimension (i.e. if
        age is ``all``, then the data represent all ages)

    For a complete list of admissible variable 3-tuples see the file
    ``can_tools/bootstrap_data/covid_variables.csv``

    For a complete list of admissible demographic 4-tuples see the file
    ``can_tools/bootstrap_data/covid_demographics.csv``

    """

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


class RequestError(Exception):
    """Error raised when a network request fails"""

    pass


class ValidateDataFailedError(Exception):
    """Error raised when data vailidation fails."""

    pass


class ValidateRelativeOrderOfCategoriesError(Exception):
    def __init__(self, category_small, category_large, problems):
        self.category_small = category_small
        self.category_large = category_large
        self.problems = problems

    def __str__(self):
        out = f"{self.category_small} > {self.category_large}\n"
        out += pprint.pformat(self.problems, indent=2)
        return out


class ValidateDecreaseInCumulativeVariableError(Exception):
    def __init__(self, category, problems):
        self.category = category
        self.problems = problems

    def __str__(self):
        out = f"{self.category} measured cumulative, but had decrease on these dates:\n"
        out += pprint.pformat(sorted(self.problems), indent=2)
        return out


class ValidationErrors(Exception):
    def __init__(self, all_errors: List[Exception]):
        self.all_errors = all_errors

    def __str__(self):
        out = "The following errors were found:"
        out += "\n" + "\n".join([str(x) for x in self.all_errors])
        return out


def _get_base_path() -> Path:
    if "DATAPATH" in os.environ.keys():
        return Path(os.environ["DATAPATH"])
    else:
        return Path.home() / ".can-data"


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

    source: str
        A string containing a URL that points to the dashboard or remote
        resource that will be scraped
    """

    autodag: bool = True
    data_type: str = "general"
    table: Type[Base]
    location_type: Optional[str]
    base_path: Path
    source: str

    # this list of tuples specifies categories for which the first
    # category listed should always be less than or equal to the second
    # category listed, when the measurement type is cumulative
    ordered_validation_categories = [
        ("total_vaccine_initiated", "total_vaccine_doses_administered"),
        ("total_vaccine_completed", "total_vaccine_doses_administered"),
        ("total_vaccine_completed", "total_vaccine_initiated"),
    ]

    def __init__(self, execution_dt: pd.Timestamp = pd.Timestamp.utcnow()):
        self.execution_dt = pd.to_datetime(execution_dt, utc=True)
        self.base_path = _get_base_path()

        # Make sure the storage path exists and create if not
        if not self.base_path.exists():
            self.base_path.mkdir(parents=True)

    @property
    def name(self) -> str:
        return self.__class__.__name__

    @classmethod
    def find_previous_fetch_execution_dates(
        cls,
        start_date: Optional[pd.Timestamp] = None,
        end_date: Optional[pd.Timestamp] = None,
        only_last: bool = False,
    ) -> List[pd.Timestamp]:
        """Finds previous times that a scraper fetch run succeeded.

        Parameters
        ----------
        start_date:
            Optional start date.
        start_date:
            Optional end date.
        only_last:
            If true will return only the most recent run.
        """
        base_path = _get_base_path()

        scraper_fetch_path = base_path / "raw" / cls.__name__

        vintages = [
            path.stem for path in scraper_fetch_path.iterdir() if path.is_file()
        ]
        timestamps = sorted(
            [pd.to_datetime(vintage, format="%Y-%m-%d_%H") for vintage in vintages]
        )

        # Filter timestamps based on input arguments.
        if start_date:
            timestamps = [
                timestamp for timestamp in timestamps if timestamp >= start_date
            ]
        if end_date:
            timestamps = [
                timestamp for timestamp in timestamps if timestamp <= end_date
            ]

        if only_last and timestamps:
            return [timestamps[-1]]

        return timestamps

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
        skip_columns: List[str] = [],
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
        skip_columns: List[str] (default=[])
            Can be set instead of ``columns`` if there are a small number
            of columns that should not be set

        Returns
        -------
        df : pd.DataFrame
            A copy of the DataFrame passed in that has new columns
            "category", "measurement", and "unit"
        """
        variable_column = df[var_name]
        out = df.copy()
        for col in columns:
            if col in skip_columns:
                continue
            out[col] = variable_column.map(lambda x: cmu[x].__getattribute__(col))
        return out

    def _filepath(self, raw: bool) -> Path:
        """
        Method for determining the file path/file name -- Everything is
        stored using the following conventions:

        * `{stage}` is either `"raw"` or `"clean"` based on whether the
          data being read in is raw or clean data
        * `{name}` comes from the name of the scraper
        * `{execution_dt}` is the execution datetime
        * `{file_type}` is the file type

        The data will then be stored  at the path:

        `{stage}/{classname}/{execution_dt}.{file_type}``

        Parameters
        ----------
        raw : bool
            Takes the value `True` if we are storing raw
            data

        Returns
        -------
        path : str
            The autogenerated name for where the data will
            be stored
        """
        # Set file path pieces
        stage = "raw" if raw else "clean"
        file_type = "pickle" if raw else "parquet"
        execution_date = self.execution_dt.strftime(r"%Y-%m-%d_%H")

        # Set filepath using its components
        path = self.base_path / stage / self.name
        if not path.exists():
            path.mkdir(parents=True)

        return path / f"{execution_date}.{file_type}"

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

    def _validate_time_series(self, df) -> List[Exception]:
        """Do checks only applicable to time series data"""
        issues = []
        if (df["measurement"] == "cumulative").sum() > 0:
            bad = timeseries.values_increasing_over_time(df)
            for variable, dates in bad.items():
                err = ValidateDecreaseInCumulativeVariableError(variable, dates)
                issues.append(err)

        return issues

    def _validate_order_of_variables(self, df) -> List[Exception]:
        """
        Returns a list of exceptions that occured while doing checks
        """
        issues = []
        cumulatives = df.query("measurement == 'cumulative'")
        categories = list(cumulatives["category"].unique())
        for category_small, category_large in self.ordered_validation_categories:
            if category_small in categories and category_large in categories:
                ok, problems = cross_section.cat1_ge_cat2(
                    cumulatives,
                    category_large,
                    category_small,
                    drop_levels=["unit", "category"],
                )
                if not ok:
                    err = ValidateRelativeOrderOfCategoriesError(
                        category_small=category_small,
                        category_large=category_large,
                        problems=problems,
                    )
                    issues.append(err)

        return issues

    def validate(self, df, df_hist):
        """
        The `validate` method checks what the tentative clean data looks
        like and then checks whether the updates are sensible -- If they
        are then it returns True otherwise it returns False and stops
        the fetch->normalize->validate->put DAG

        Raises Exceptions if validation fails

        Parameters
        ----------
        df : pd.DataFrame
            The data that is being proposed for put
        df_hist : pd.DataFrame
            Historical data

        """
        # TODO (SL, 2021-04-12): we aren't ready with other tooling for dealing
        # nicely with validation errors and exception lists. Until then we will
        # disable validation
        return
        issues = []
        if utils.is_time_series(df):
            issues.extend(self._validate_time_series(df))

        issues.extend(self._validate_order_of_variables(df))

        if len(issues) > 0:
            raise ValidationErrors(issues)

    def _validate(self):
        """
        The `_validate` method loads the appropriate data and then
        checks the data using `validate` and determines whether the
        data has been validated
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

    def find_unknown_demographic_id(self, engine: Engine, df: pd.DataFrame):
        dems = pd.read_sql("select * from covid_demographics", engine)
        merged = df.merge(dems, on=["sex", "age", "race", "ethnicity"], how="left")
        bad = list(merged["id"].isna())
        return df.loc[bad, :]

    def fetch_normalize(self):
        "Call `self.normalize(self.fetch())`"
        data = self.fetch()
        return self.normalize(data)

    def reprocess_from_already_fetched_data(self, engine: Engine):
        """Reprocesses data from fetched data - useful to fix errors after fetch."""
        self._normalize()

        # validate will either succeed or raise an Exception
        self._validate()

        self._put(engine)
