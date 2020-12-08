import asyncio
import os
from typing import List

import requests
import sqlalchemy as sa
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# set up sqlalchemy
CONN_STR = os.environ.get(
    "SQL_CONN_STR",
    "postgresql://localhost:5432/covid",
)
CLEAN_SWAGGER_URL = os.environ.get("CLEAN_SWAGGER_URL", None)
if CLEAN_SWAGGER_URL is None:
    raise ValueError("The CLEAN_SWAGGER_URL environment variable not found")

engine = sa.create_engine(CONN_STR)
print(engine)
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


class Dataset(BaseModel):
    name: str
    variables: List[str]


async def _get_variable_names_for_endpoint(endpoint: str) -> Dataset:
    print("Starting work on endpoint:", endpoint)
    default_sql = f"select distinct(variable) from api.{endpoint}"
    if endpoint == "usafacts_covid" or endpoint == "nytimes_covid":
        return Dataset(name=endpoint, variables=["deaths_total", "cases_total"])
    sql = {
        "covid_us": "select name from meta.covid_variables",
        "economics": "select variable_name from data.dol_ui group by 1",
        "npi_us": "select distinct(name) from meta.npi_variables",
        # "nytimes_covid": """
        # select distinct(name)
        # from data.nyt_covid us
        # left join meta.covid_variables cv on cv.id = us.variable_id
        # """,
        # "usafacts_covid": """
        # select distinct(name)
        # from data.usafacts_covid us
        # left join meta.covid_variables cv on cv.id = us.variable_id
        # """,
    }
    sql["covid_historical"] = sql["covid_us"]
    query = sql.get(endpoint, default_sql)
    res = engine.execute(query)
    variables = [x[0] for x in res.fetchall()]
    if endpoint == "economics":
        variables += ["wei"]
    print("finished work on endpoint:", endpoint)
    return Dataset(name=endpoint, variables=variables)
    pass


async def _get_variable_names() -> List[Dataset]:
    res_swagger = requests.get(CLEAN_SWAGGER_URL)
    if not res_swagger.ok:
        raise ValueError("Could not request swagger file")

    swagger = res_swagger.json()
    coroutines = []
    for endpoint, props in swagger["paths"].items():
        if not "parameters" in props["get"]:
            continue
        for param in props["get"]["parameters"]:
            if isinstance(param, dict):
                for val in param.values():
                    if "variable" in val:
                        coroutines.append(
                            _get_variable_names_for_endpoint(endpoint.strip("/"))
                        )

    datasets = await asyncio.gather(*coroutines)
    return datasets


@app.get("/variable_names")
async def get_variable_names() -> List[Dataset]:
    return await _get_variable_names()
