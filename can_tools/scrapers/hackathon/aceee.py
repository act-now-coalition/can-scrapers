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

    def fetch(self, pages: str):
        return (
            [table.df for table in camelot.read_pdf(HACKATHON_DATA_PATH, pages=pages, flavor="stream")]
        )


    def combine_and_persist_data(self, pages: str, filename: str):
        tables: List[pd.DataFrame] = []
        for table in self.fetch(pages):
            start_idx = table.index[table.iloc[:, 0] == "Rank"].values[0]
            tables.append(table[start_idx + 1:])
        data = pd.concat(tables)
        data.columns = self.SUMMARY_COLUMNS
        data = (
            data.loc[~data["city"].isin(LOCATIONS_TO_DROP)]
            .replace(LOCATIONS_TO_RENAME)
            .assign(
                city=lambda row: row["city"].str.lower().str.replace(" ", "-").str.replace(".", ""),
                location=lambda row: row["state"].str.lower() + "-" + row["city"]
            )
        )

        # See TODO above
        data = data.loc[:, [col for col in data.columns if col != "drop"]]
        persist_data(data=data, filename=filename)
        return data


def persist_data(data: pd.DataFrame, filename: str):
    data.to_csv(f"/Users/sean/Documents/can-scrapers/can_tools/scrapers/hackathon/data/{filename}", index=False)
