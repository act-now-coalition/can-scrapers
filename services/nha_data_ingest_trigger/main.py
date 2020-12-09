import os

import requests


def nha_data_ingest_trigger(event, context):
    """
    Background Cloud Function to be triggered by Cloud Storage.

    Yay documentation.... Make Chase write this later

    Parameters
    ----------
        event (dict):  The dictionary with data specific to this type of event.
                       The `data` field contains a description of the event in
                       the Cloud Storage `object` format described here:
                       https://cloud.google.com/storage/docs/json_api/v1/objects#resource
        context (google.cloud.functions.Context): Metadata of triggering event.

    Returns
    -------
        None
    """
    # URL to place post to
    url = os.environ.get(
        "NHA_SERVICE_URL", "https://can-nha-reports-inunbrtacq-uk.a.run.app/nha"
    )
    bucket, name = event["bucket"], event["name"]
    print(f"Posting {bucket}:{name}")

    res = requests.post(url, json={"bucket": bucket, "name": name})
    print(f"The post status was: {res.status_code}")

    return None
