from pathlib import Path
from typing import Dict, List, Tuple, Type
import sqlalchemy as sa
from sqlalchemy import (
    BigInteger,
    Column,
    Date,
    DateTime,
    ForeignKey,
    Integer,
    Numeric,
    String,
    event,
)
from sqlalchemy.engine.base import Engine
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy.schema import CreateSchema
from sqlalchemy.sql.expression import null, select
from sqlalchemy.sql.functions import func
from sqlalchemy.orm import backref, relationship, sessionmaker
from sqlalchemy_utils import create_view
import pandas as pd

Base = declarative_base()


for schema in ["meta", "data"]:
    event.listen(
        Base.metadata,
        "before_create",
        CreateSchema(schema).execute_if(dialect="postgresql"),
    )


class MetaSchemaMixin:
    __table_args__ = {"schema": "meta"}


class DataSchemaMixin:
    __table_args__ = {"schema": "data"}


class APISchemaMixin:
    __table_args__ = {"schema": "api"}


class LocationType(Base, MetaSchemaMixin):
    __tablename__ = "location_types"
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True)


class Location(Base, MetaSchemaMixin):
    __tablename__ = "locations"
    id = Column(Integer, primary_key=True)
    location = Column(BigInteger, nullable=False, unique=True)
    location_type_id = Column(Integer, ForeignKey(LocationType.id), nullable=False)
    state = Column(String)
    name = Column(String, nullable=False)
    area = Column(Numeric)
    latitude = Column(Numeric)
    longitude = Column(Numeric)
    fullname = Column(String, nullable=False)

    # location_type = relationship(
    #     LocationType, backref=backref("locations", uselist=True, cascade="delete,all")
    # )


class CovidCategory(Base, MetaSchemaMixin):
    __tablename__ = "covid_categories"
    group = Column(String, nullable=False)
    category = Column(String, unique=True, primary_key=True, index=True)


class CovidMeasurement(Base, MetaSchemaMixin):
    __tablename__ = "covid_measurements"
    name = Column(String, primary_key=True)


class CovidUnit(Base, MetaSchemaMixin):
    __tablename__ = "covid_units"
    name = Column(String, primary_key=True)


class CovidVariable(Base, MetaSchemaMixin):
    __tablename__ = "covid_variables"
    id = Column(Integer, primary_key=True)
    category = Column(String, ForeignKey(CovidCategory.category))
    measurement = Column(String, ForeignKey(CovidMeasurement.name))
    unit = Column(String, ForeignKey(CovidUnit.name))

    official_obs = relationship("CovidOfficial", backref="variable")


class CovidDemographic(Base, MetaSchemaMixin):
    __tablename__ = "covid_demographics"
    id = Column(Integer, primary_key=True)
    age = Column(String)
    race = Column(String)
    sex = Column(String)
    official_obs = relationship("CovidOfficial", backref="demographic")


class CovidProvider(Base, MetaSchemaMixin):
    __tablename__ = "covid_providers"
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)
    priority = Column(Integer, nullable=False)
    official_obs = relationship("CovidOfficial", backref="provider")


class _ObservationBase(DataSchemaMixin):
    dt = Column(Date, primary_key=True)

    @declared_attr
    def location_id(cls):
        return Column(Integer, ForeignKey(Location.location), primary_key=True)

    @declared_attr
    def variable_id(cls):
        return Column(Integer, ForeignKey(CovidVariable.id), primary_key=True)

    @declared_attr
    def demographic_id(cls):
        return Column(Integer, ForeignKey(CovidDemographic.id), primary_key=True)

    @declared_attr
    def provider_id(cls):
        return Column(Integer, ForeignKey(CovidProvider.id))

    @declared_attr
    def last_updated(cls):
        return Column(DateTime, nullable=False, default=func.now())

    value = Column(Numeric)


class CovidObservation(Base, _ObservationBase):
    __tablename__ = "covid_observations"


class CovidOfficial(Base, _ObservationBase):
    __tablename__ = "covid_official"
    # variable = relationship(CovidVariable)
    # demographic = relationship(CovidVariable)
    # provider = relationship(CovidVariable)


class CovidUSAFacts(Base, _ObservationBase):
    __tablename__ = "covid_usafacts"


api_covid_us_statement = select(
    [
        CovidProvider.name.label("provider"),
        CovidOfficial.dt,
        Location.location,
        CovidVariable.category.label("variable_name"),
        CovidVariable.measurement,
        CovidVariable.unit,
        CovidDemographic.age,
        CovidDemographic.race,
        CovidDemographic.sex,
        CovidOfficial.last_updated,
        CovidOfficial.value,
    ]
).select_from(
    (
        CovidOfficial.__table__.join(CovidVariable, isouter=True)
        .join(Location, isouter=True)
        .join(CovidProvider, isouter=True)
        .join(CovidDemographic, isouter=True)
    )
)

api_covid_us = create_view("covid_us", api_covid_us_statement, Base.metadata)


class CovidUS(Base, APISchemaMixin):
    __table__ = api_covid_us


def _bootstrap_csv_to_orm(cls: Type[Base]):
    fn = cls.__tablename__ + ".csv"
    path = Path(__file__).parent / "bootstrap_data" / fn
    records = pd.read_csv(path).to_dict(orient="records")
    return [cls(**x) for x in records]


def bootstrap(sess) -> Dict[str, List[Base]]:
    components = dict(
        location_types=_bootstrap_csv_to_orm(LocationType),
        categories=_bootstrap_csv_to_orm(CovidCategory),
        measurements=_bootstrap_csv_to_orm(CovidMeasurement),
        units=_bootstrap_csv_to_orm(CovidUnit),
        demographics=_bootstrap_csv_to_orm(CovidDemographic),
        providers=_bootstrap_csv_to_orm(CovidProvider),
    )

    sess.add_all([row for table in components.values() for row in table])
    sess.commit()

    # now that we have categories, measurements, units we can populate
    # variables
    components["variables"] = _bootstrap_csv_to_orm(CovidVariable)
    sess.add_all(components["variables"])
    sess.commit()

    # need to handle locations separately b/c foreign key
    fn = Location.__tablename__ + ".csv"
    path = Path(__file__).parent / "bootstrap_data" / fn
    records = pd.read_csv(path).to_dict(orient="records")
    location_types = {v.name: v for v in components["location_types"]}
    locations = []
    for row in records:
        location_type_id = location_types[row.pop("location_type")].id
        locations.append(Location(location_type_id=location_type_id, **row))
    components["locations"] = locations

    sess.add_all(locations)
    sess.commit()

    return components


def create_dev_engine() -> Tuple[Engine, sessionmaker]:
    engine = sa.create_engine(
        "sqlite:///:memory:",
        echo=True,
        execution_options={
            "schema_translate_map": {"meta": None, "data": None, "api": None}
        },
    )
    Session = sessionmaker(bind=engine)

    return engine, Session
