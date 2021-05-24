from contextlib import closing
from pathlib import Path
from typing import Dict, List, Tuple, Type, Union

# from sqlalchemy_utils import create_view
import pandas as pd
import sqlalchemy as sa
from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    event,
)
from sqlalchemy.engine.base import Engine
from sqlalchemy.ext import compiler
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.sql.ddl import DDL, DDLElement
from sqlalchemy.sql.expression import and_, select
from sqlalchemy.sql.functions import func
from sqlalchemy.sql.schema import (
    ForeignKeyConstraint,
    PrimaryKeyConstraint,
    UniqueConstraint,
)
from sqlalchemy_utils.view import DropView, create_table_from_selectable

Base = declarative_base()


def FKCascade(*args, **kwargs) -> ForeignKey:
    return ForeignKey(*args, **kwargs, ondelete="CASCADE")


class CreateView(DDLElement):
    def __init__(self, name, selectable, or_replace=False, materialized=False):
        self.name = name
        self.selectable = selectable
        self.materialized = materialized
        self.or_replace = or_replace


@compiler.compiles(CreateView, "sqlite")
def compile_create_materialized_view(element, compiler, **kw):
    return "CREATE {m}VIEW {r} {n} AS {s}".format(
        m="MATERIALIZED " if element.materialized else "",
        r="IF NOT EXISTS " if element.or_replace else "",
        n=element.name,
        s=compiler.sql_compiler.process(element.selectable, literal_binds=True),
    )


@compiler.compiles(CreateView)
def compile_create_materialized_view(element, compiler, **kw):
    return "CREATE {r} {m}VIEW {n} AS {s}".format(
        r="OR REPLACE " if element.or_replace else "",
        m="MATERIALIZED " if element.materialized else "",
        n=element.name,
        s=compiler.sql_compiler.process(element.selectable, literal_binds=True),
    )


def create_view(
    name,
    selectable,
    metadata,
    or_replace=False,
    materialized=False,
    cascade_on_drop=True,
):
    table = create_table_from_selectable(
        name=name, selectable=selectable, metadata=None
    )
    CV = CreateView(name, selectable, or_replace, materialized)
    sa.event.listen(metadata, "after_create", CV)

    @sa.event.listens_for(metadata, "after_create")
    def create_indexes(target, connection, **kw):
        for idx in table.indexes:
            idx.create(connection)

    sa.event.listen(metadata, "before_drop", DropView(name, cascade=cascade_on_drop))
    return table


for schema in ["meta", "data"]:
    event.listen(
        Base.metadata,
        "before_create",
        DDL("CREATE SCHEMA IF NOT EXISTS {}".format(schema)).execute_if(
            dialect="postgresql"
        ),
    )


class MetaSchemaMixin:
    __table_args__ = {"schema": "meta"}


class DataSchemaMixin:
    __table_args__ = {"schema": "data"}


class APISchemaMixin:
    __table_args__ = {"schema": "api"}


class Location(Base, MetaSchemaMixin):
    __tablename__ = "locations"
    id = Column(String, primary_key=True)
    location = Column(BigInteger, nullable=False)
    location_type = Column(String, nullable=False)
    state_fips = Column(Integer)
    state = Column(String)
    name = Column(String, nullable=False)
    area = Column(Numeric)
    latitude = Column(Numeric)
    longitude = Column(Numeric)
    fullname = Column(String, nullable=False)

    __table_args__ = (
        UniqueConstraint(location, location_type, sqlite_on_conflict="IGNORE"),
        UniqueConstraint(location_type, state_fips, name, name="uix_1"),
        {"schema": "meta"},
    )

    # location_type = relationship(
    #     LocationType, backref=backref("locations", uselist=True, cascade="delete,all")
    # )


class CovidCategory(Base, MetaSchemaMixin):
    __tablename__ = "covid_categories"
    group = Column(String, nullable=False)
    category = Column(
        String,
        unique=True,
        primary_key=True,
        index=True,
        sqlite_on_conflict_primary_key="IGNORE",
    )


class CovidMeasurement(Base, MetaSchemaMixin):
    __tablename__ = "covid_measurements"
    name = Column(
        String,
        primary_key=True,
        sqlite_on_conflict_primary_key="IGNORE",
    )


class CovidUnit(Base, MetaSchemaMixin):
    __tablename__ = "covid_units"
    name = Column(
        String,
        primary_key=True,
        sqlite_on_conflict_primary_key="IGNORE",
    )


class CovidVariable(Base):
    __tablename__ = "covid_variables"
    id = Column(
        Integer,
        primary_key=True,
        sqlite_on_conflict_primary_key="IGNORE",
    )
    category = Column(String, FKCascade(CovidCategory.category))
    measurement = Column(String, FKCascade(CovidMeasurement.name))
    unit = Column(String, FKCascade(CovidUnit.name))

    official_obs = relationship("CovidObservation", backref="variable")

    __table_args__ = (
        UniqueConstraint(category, measurement, unit, name="uix_variables"),
        {"schema": "meta"},
    )


class CovidDemographic(Base):
    __tablename__ = "covid_demographics"
    id = Column(
        Integer,
        primary_key=True,
        sqlite_on_conflict_primary_key="IGNORE",
    )
    age = Column(String)
    race = Column(String)
    ethnicity = Column(String)
    sex = Column(String)
    official_obs = relationship("CovidObservation", backref="demographic")

    __table_args__ = (
        UniqueConstraint(age, race, ethnicity, sex, name="uix_demo"),
        {"schema": "meta"},
    )


class CovidProvider(Base, MetaSchemaMixin):
    __tablename__ = "covid_providers"
    id = Column(
        Integer,
        primary_key=True,
        sqlite_on_conflict_primary_key="IGNORE",
    )
    name = Column(String, unique=True, nullable=False)
    priority = Column(Integer, nullable=False)
    official_obs = relationship("CovidObservation", backref="provider")


class CovidObservation(Base):
    __tablename__ = "covid_observations"
    dt = Column(Date)
    location_id = Column(String, FKCascade(Location.id))
    variable_id = Column(Integer, FKCascade(CovidVariable.id))
    demographic_id = Column(Integer, FKCascade(CovidDemographic.id))
    provider_id = Column(Integer, FKCascade(CovidProvider.id))
    value = Column(Numeric)
    last_updated = Column(DateTime, nullable=False, default=func.now())
    source_url = Column(String)
    source_name = Column(String)
    deleted = Column(Boolean, nullable=False, default=False)
    __table_args__ = (
        PrimaryKeyConstraint(
            "dt",
            "location_id",
            "variable_id",
            "demographic_id",
            "provider_id",
            sqlite_on_conflict="REPLACE",
        ),
        {"schema": "data"},
    )


Index("covid_observation_deleted", CovidObservation.deleted)

api_covid_us_statement = select(
    [
        CovidProvider.name.label("provider"),
        CovidObservation.dt,
        Location.id.label("location_id"),
        Location.location,
        Location.location_type,
        CovidVariable.category.label("variable_name"),
        CovidVariable.measurement,
        CovidVariable.unit,
        CovidDemographic.age,
        CovidDemographic.race,
        CovidDemographic.ethnicity,
        CovidDemographic.sex,
        CovidObservation.last_updated,
        CovidObservation.source_url,
        CovidObservation.source_name,
        CovidObservation.value,
    ]
).select_from(
    (
        CovidObservation.__table__
        .join(CovidVariable, isouter=True)
        .join(Location, isouter=True)
        .join(CovidProvider, isouter=True)
        .join(CovidDemographic, isouter=True)
    )
).where(CovidObservation.deleted == False)

api_covid_us = create_view(
    "covid_us", api_covid_us_statement, Base.metadata, or_replace=True
)


class CovidUS(Base, APISchemaMixin):
    __table__ = api_covid_us


class _TempOfficial:
    id = Column(Integer, primary_key=True, sqlite_on_conflict_primary_key="IGNORE")
    location_type = Column(String)
    dt = Column(Date, nullable=False)
    age = Column(String, nullable=False)
    race = Column(String, nullable=False)
    ethnicity = Column(String, nullable=False)
    sex = Column(String, nullable=False)
    last_updated = Column(DateTime, nullable=False, default=func.now())
    source_url = Column(String)
    source_name = Column(String)

    @declared_attr
    def provider(cls):
        return Column(String, FKCascade(CovidProvider.name), nullable=False)

    @declared_attr
    def category(cls):
        return Column(String, FKCascade(CovidCategory.category), nullable=False)

    @declared_attr
    def measurement(cls):
        return Column(String, FKCascade(CovidMeasurement.name), nullable=False)

    @declared_attr
    def unit(cls):
        return Column(String, FKCascade(CovidUnit.name), nullable=False)

    insert_op = Column(String, nullable=False)
    value = Column(Numeric, nullable=False)


class TemptableOfficialHasLocation(Base, _TempOfficial, DataSchemaMixin):
    __tablename__ = "temp_official_has_location"
    location = Column(
        BigInteger,
        nullable=False,
    )

    __table_args__ = (
        ForeignKeyConstraint(
            ["age", "race", "ethnicity", "sex"],
            [
                CovidDemographic.age,
                CovidDemographic.race,
                CovidDemographic.ethnicity,
                CovidDemographic.sex,
            ],
        ),
        ForeignKeyConstraint(
            ["location", "location_type"], [Location.location, Location.location_type]
        ),
        {"schema": "data"},
    )


class TemptableOfficialNoLocation(Base, _TempOfficial, DataSchemaMixin):
    __tablename__ = "temp_official_no_location"
    state_fips = Column(Integer)
    location_name = Column(String)

    __table_args__ = (
        ForeignKeyConstraint(
            ["age", "race", "ethnicity", "sex"],
            [
                CovidDemographic.age,
                CovidDemographic.race,
                CovidDemographic.ethnicity,
                CovidDemographic.sex,
            ],
        ),
        ForeignKeyConstraint(
            ["location_type", state_fips, location_name],
            [Location.location_type, Location.state_fips, Location.name],
        ),
        {"schema": "data"},
    )


def build_insert_from_temp(
    insert_op: str,
    cls: Union[Type[TemptableOfficialNoLocation], Type[TemptableOfficialHasLocation]],
    engine: Engine,
):
    print("Have insert_op = ", insert_op)
    columns = [
        cls.dt,
        Location.id.label("location_id"),
        CovidVariable.id.label("variable_id"),
        CovidDemographic.id.label("demographic_id"),
        cls.value,
        CovidProvider.id.label("provider_id"),
        cls.last_updated,
        cls.source_url,
        cls.source_name,
    ]
    selector = (
        select(columns)
        .where(cls.insert_op == insert_op)
        .select_from(
            (
                cls.__table__.join(Location, isouter=True)
                .join(CovidProvider, isouter=True)
                .join(CovidDemographic, isouter=True)
                .join(
                    CovidVariable,
                    and_(
                        cls.category == CovidVariable.category,
                        cls.measurement == CovidVariable.measurement,
                        cls.unit == CovidVariable.unit,
                    ),
                    isouter=True,
                )
            )
        )
    )
    covid_table = CovidObservation.__table__
    if "postgres" in engine.dialect.name:
        from sqlalchemy.dialects.postgresql import insert

        ins = insert(covid_table)
        statement = ins.from_select([x.name for x in columns], selector)

        return statement.on_conflict_do_update(
            index_elements=[x.name for x in covid_table.primary_key.columns],
            set_=dict(
                value=statement.excluded.value,
                last_updated=statement.excluded.last_updated,
                provider_id=statement.excluded.provider_id,
            ),
        )

    ins = covid_table.insert()
    return ins.from_select([x.name for x in columns], selector)


def _bootstrap_csv_to_orm(cls: Type[Base], engine: Engine):
    fn = cls.__tablename__ + ".csv"
    path = Path(__file__).parent / "bootstrap_data" / fn
    records = pd.read_csv(path).to_dict(orient="records")
    rows = [cls(**x) for x in records]
    if "postgres" in engine.dialect.name:
        from sqlalchemy.dialects.postgresql import insert

        ins = insert(
            cls.__table__, values=records, bind=engine
        ).on_conflict_do_nothing()
        return ins, rows
    else:
        ins = cls.__table__.insert(values=records, bind=engine)
        return ins, rows


def bootstrap(
    sess: sa.orm.session.Session, delete_first: bool = True
) -> Dict[str, List[Base]]:
    tables: List[Type[Base]] = [
        CovidCategory,
        CovidMeasurement,
        CovidUnit,
        CovidDemographic,
        CovidProvider,
        Location,
        CovidVariable,
    ]

    if sess.bind is None:
        raise ValueError("Session must be bound to an engine or connection")

    # drop in reverse order to avoid constraint issues
    if delete_first:
        for t in tables[::-1]:
            # first delete from table
            sess.execute(t.__table__.delete())
            sess.commit()

    components = {}
    for t in tables:
        ins, rows = _bootstrap_csv_to_orm(t, sess.bind)
        components[t.__tablename__] = rows
        ins.execute()

    return components


def create_dev_engine(
    verbose: bool = True, path: str = "/:memory:"
) -> Tuple[Engine, sessionmaker]:
    """Create an in memory sqlite version of the CAN database for testing

    Args:
        verbose (bool, optional): Whether or not to print all executed
                                  SQL statements to stdout. Defaults to True.
        path (str, optional): Path or location for sqlite database.
                              Defaults to "/:memory:", which means the database
                              will live in the Python session memory

    Returns:
        Tuple[Engine, sessionmaker]: SQLAlchemy engine and sessionmaker instance
    """
    engine = sa.create_engine(
        f"sqlite://{path}",
        echo=verbose,
        execution_options={
            "schema_translate_map": {"meta": None, "data": None, "api": None}
        },
    )
    Session = sessionmaker(bind=engine)
    Base.metadata.create_all(engine)
    with closing(Session()) as sess:
        bootstrap(sess)

    return engine, Session
