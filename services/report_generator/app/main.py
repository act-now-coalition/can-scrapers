import datetime
import os
from typing import List, Dict, Optional

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
from pydantic import BaseModel

API_URL = os.environ.get("API_URL", None)
if API_URL is None:
    raise ValueError("API_URL not known")

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["*"],
)



class CountyReport(BaseModel):
    name: str
    fips: Optional[int]
    sources: Dict[str, List[str]]  # {url: [var1, var2]}


class StateReport(CountyReport):
    counties: List[CountyReport]


class Reports(BaseModel):
    week_end: datetime.datetime
    week_start: datetime.datetime
    updates: List[StateReport]


def aggregate_week_fips_updates(x):
    return pd.Series(
        {
            "state": x["state"].iloc[0],
            "name": x["fullname"].iloc[0],
            "sources": (
                x.groupby("source").apply(lambda y: list(y["variable_name"])).to_dict()
            ),
        }
    )


def aggregate_week_updates(week_state):
    out = {}
    out["name"] = state = week_state.name[1]
    state_only = week_state.query("name == @state")
    out["sources"] = {}
    for ss in list(state_only["sources"]):
        out["sources"].update(ss)

    non_state: pd.DataFrame = week_state.query("name != @state")
    out["counties"] = []

    def group_county_sources(c_df: pd.DataFrame):
        c_out = {
            "name": c_df["name"].iloc[0],
            "fips": c_df["fips"].iloc[0],
            "sources": {},
        }
        for c_src in list(c_df["sources"]):
            c_out["sources"].update(c_src)
        return c_out

    out["counties"] = list(non_state.groupby("fips").apply(group_county_sources))
    return pd.Series(out)


def flatten_by_date(x):
    return x.drop(["start_date", "state"], axis=1).to_dict(orient="records")



def get_reports():
    df = pd.read_json(f"{API_URL}/us_covid_variable_start_date")
    df["start_date"] = pd.to_datetime(df["start_date"])

    date_chunks = (
        df.groupby([pd.Grouper(key="start_date", freq="W-MON"), "fips", "source"])
        .apply(aggregate_week_fips_updates)
        .reset_index()
        .groupby(["start_date", "state"])
        .apply(aggregate_week_updates)
        .reset_index()
        .groupby("start_date")
        .apply(flatten_by_date)
    )
    out = []
    for sd in date_chunks.index:
        out.append(
            {
                "week_end": sd,
                "week_start": (sd - pd.Timedelta(days=7)),
                "updates": date_chunks.loc[sd],
            }
        )

    print("type(out)", type(out))
    print("type(out[0])", type(out[0]))
    print("type(out[1]['updates'][0]", type(out[1]["updates"][0]["sources"]))

    return out


@app.get("/reports", response_model=List[Reports], response_model_exclude_unset=True)
async def reports() -> List[Reports]:
    return get_reports()
