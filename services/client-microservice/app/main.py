import io
from typing import Optional

import covidcountydata as ccd
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from pydantic import BaseModel

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["POST"],
    allow_headers=["*"],
)


class ClientRequest(BaseModel):
    apikey: Optional[str]
    parameters: dict


def get_data(request: ClientRequest):
    client = ccd.Client(request.apikey)
    for endpoint, options in request.parameters.items():
        getattr(client, endpoint)(**options)

    data = client.fetch()
    return data


@app.post(
    "/client-request",
)
async def reports(request: ClientRequest) -> Response:
    df = get_data(request)
    print("This is the df:", type(df))
    with io.StringIO() as data_buf:
        df.to_csv(data_buf, index=False)
        data = data_buf.getvalue()
    print("this is data: ", data)
    out = Response(data, status_code=200, media_type="text/csv")
    return out
