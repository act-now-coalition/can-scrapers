from typing import List
import camelot
import pandas as pd
from google.cloud import storage

# TODO: use paths
HACKATHON_DATA_PATH = "/Users/sean/Documents/can-scrapers/can_tools/scrapers/hackathon/data/aceee-report-downgrade.pdf"
LOCATIONS_TO_DROP = ["Springs"]
LOCATIONS_TO_RENAME = {"Colorado": "Colorado Springs", "San José": "San Jose"}


class AceeCityReport:

    # TODO: handle drop columns better
    SUMMARY_COLUMNS = ["rank", "city", "state", "community-wide initiatives", "building", "transportation", "energy and water", "local government", "total", "drop", "drop", "drop"]

    def fetch(self):
        return (
            [table.df for table in camelot.read_pdf(HACKATHON_DATA_PATH, pages="22-24", flavor="stream")]
        )


    def combine_data(self):
        tables: List[pd.DataFrame] = []
        for table in self.fetch():
            start_idx = table.index[table.iloc[:, 0] == "Rank"].values[0]
            tables.append(table[start_idx + 1:])
        data = pd.concat(tables)
        data.columns = self.SUMMARY_COLUMNS
        data = (
            data.loc[~data["city"].isin(LOCATIONS_TO_DROP)]
            .rename(LOCATIONS_TO_RENAME)
            .assign(
                city=lambda row: row["city"].str.lower().str.replace(" ", "-").str.replace(".", ""),
                location=lambda row: row["state"].str.lower() + "-" + row["city"]
            )
        )

        # See TODO above
        return data.loc[:, [col for col in data.columns if col != "drop"]]

    @staticmethod
    def persist_data(data: pd.DataFrame, fname: str):
        data.to_csv(f"/Users/sean/Documents/can-scrapers/can_tools/scrapers/hackathon/data/{fname}", index=False)

    @staticmethod
    def upload_blob(source_file_name, destination_blob_name):
        """Uploads a file to the bucket."""
        # The ID of your GCS bucket
        # bucket_name = "your-bucket-name"
        # The path to your file to upload
        # source_file_name = "local/path/to/file"
        # The ID of your GCS object
        # destination_blob_name = "storage-object-name"

        F = "/Users/sean/Documents/can-scrapers/can_tools/scrapers/hackathon/data/"

        storage_client = storage.Client()
        bucket = storage_client.bucket("actnow-hackathon-aceee")
        blob = bucket.blob(destination_blob_name)

        blob.upload_from_filename(f"{F}{source_file_name}")

        print(
            f"File {source_file_name} uploaded to {destination_blob_name}."
        )
