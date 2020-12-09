import datetime
import io
import os
import tempfile
import textwrap

import pandas as pd
import sqlalchemy as sa
import sqlalchemy.exc
from cmdc_tools.datasets import NVHospitalPdf
from fastapi import FastAPI, HTTPException
from google.cloud import storage
from pydantic import BaseModel

# set up sqlalchemy
CONN_STR = os.environ.get("SQL_CONN_STR", "postgresql://localhost:5432/covid")
engine = sa.create_engine(CONN_STR)
app = FastAPI()


class Event(BaseModel):
    bucket: str
    name: str


@app.post("/nha")
async def upload_nha_report(event: Event):
    """
    This is a google function that is called whenever there is a
    google cloud storage finalize event (which happens when a new file
    is created or replaced)

    This happens when we manually upload the Nevada Hospital
    Association reports

    Parameters
    ----------
    event : Event
        The dictionary with data specific to this type of event.
        The `data` field contains a description of the event in
        the Cloud Storage `object` format described here:
        https://cloud.google.com/storage/docs/json_api/v1/objects#resource
    """
    # Get relevant information from messages
    bucket = event.bucket
    file_name = event.name
    print(f"Getting data from {bucket}/{name}")

    # Create google cloud storage client
    client = storage.Client()

    # Get the bucket/blob
    bucket = client.get_bucket(bucket)
    blob = bucket.get_blob(file_name)

    # Once we have the blob, download the bytes and turn
    # into a temporary file
    print("Opening temp directory")
    with tempfile.TemporaryDirectory() as tmpdir:
        with open(f"{tmpdir}/nhafile.pdf", "wb") as f:
            print("Downloading file")
            blob.download_to_file(f)

        # Now create the NV hospital pdf parser and parse the
        # data into a dataframe
        print("Writing to file")
        nvh = NVHospitalPdf()
        df = nvh.get(f"{tmpdir}/nhafile.pdf")

    print("Putting data into database")
    connstr = sa.create_engine()
    nvh.put(CONN_STR, df)

    return None
