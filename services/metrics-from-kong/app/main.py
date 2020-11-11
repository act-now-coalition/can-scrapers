import datetime
import os

from fastapi import FastAPI
from pydantic import BaseModel
import sqlalchemy as sa
from mixpanel import Mixpanel

CONN_STR = os.environ.get("SQL_CONN_STR", "postgresql://localhost:5432/covid")
engine = sa.create_engine(CONN_STR)
print(engine)

app = FastAPI()

MP_TOKEN = os.environ.get("MIXPANEL_TOKEN", None)
if MP_TOKEN is None:
    raise ValueError("Mixpanel token not found")
MP = Mixpanel(MP_TOKEN)


class Consumer(BaseModel):
    created_at: int
    id: str  # todo do they have uuid type?
    username: str


class Latencies(BaseModel):
    request: int
    kong: int
    proxy: int


class Service(BaseModel):
    host: str
    created_at: int
    connect_timeout: int
    id: str
    protocol: str
    name: str
    read_timeout: int
    port: int
    updated_at: int
    write_timeout: int
    retries: int


class Response(BaseModel):
    headers: dict
    status: int
    size: str


class Request(BaseModel):
    querystring: dict
    size: str
    uri: str
    url: str
    headers: dict
    method: str


class Metric(BaseModel):
    consumer: Consumer
    latencies: Latencies
    service: Service
    request: Request
    client_ip: str
    tries: list
    upstream_uri: str
    response: Response
    route: dict
    started_at: int


@app.post("/metrics")
async def metrics(metric: Metric):
    print("here is my metric!", metric)
    ip = metric.request.headers.get("x-forwarded-for", metric.client_ip)
    values = dict(
        username=metric.consumer.username,
        request_latency=metric.latencies.request,
        kong_latency=metric.latencies.kong,
        proxy_latency=metric.latencies.proxy,
        request_url=metric.request.url,
        client_ip=ip,
        response_status=metric.response.status,
        response_size=int(metric.response.size),
        started_at=datetime.datetime.fromtimestamp(metric.started_at / 1000),
        user_agent=metric.request.headers.get("user-agent", None),
    )
    print("these are the values:", values)
    sql = sa.text(
        """
    INSERT INTO metrics.requests (username, request_latency, kong_latency, proxy_latency, request_url, client_ip, response_status, response_size, started_at, user_agent) VALUES(:username, :request_latency, :kong_latency, :proxy_latency, :request_url, :client_ip, :response_status, :response_size, :started_at, :user_agent)
    """
    )
    engine.execute(sql, **values)

    # send to mixpanel also
    distinct_id = metric.consumer.id
    values["uri"] = metric.request.uri
    MP.track(distinct_id, "api request", properties=values, meta={"ip": ip})

    return "success"
