import logging
import os
from flask import Flask, abort, make_response
from flask_cors import CORS
import datetime
import requests

# Change the format of messages logged to Stackdriver
logging.basicConfig(format="%(message)s", level=logging.INFO)

app = Flask(__name__)
CORS(app)

BUCKET_NAME = os.environ.get("GCP_BUCKET", None)
if BUCKET_NAME is None:
    raise ValueError("BUCKET_NAME environment variable not found")


@app.route("/<dataset>")
def get_recent_dataset(dataset):
    # Get most recent file for reqeusted dataset
    url = f"https://storage.googleapis.com/{BUCKET_NAME}/downloadables/" + dataset

    res = requests.get(url)

    if not res.ok:
        abort(404)

    dataset_name = dataset.split(".")[0].replace("_latest", "")
    ext = dataset.split(".", 1)[-1]
    today = datetime.datetime.utcnow()
    filename = f"{dataset_name}_{today:%Y-%m-%d}.{ext}"

    out = make_response(res.content)
    ct = "application/zip" if dataset.endswith(".csv.zip") else "text/csv"
    out.headers.set("Content-Type", ct)
    out.headers.set("Content-Disposition", f"attachment; filename={filename}")
    return out


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
