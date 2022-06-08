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
    SUMMARY_COLUMNS = ["rank", "city", "state", "community_wide_initiatives", "building", "transportation", "energy_and_water", "local_government", "total", "score_delta_2020", "rank_delta_2020", "drop"]
    CITY_INFO_COLUMNS = [
        "city", "state", "msa_population", "msa_classification", "10yr_avg_annual_population_change", "growth_classification"
    ]

    def fetch(self, pages: str):
        return (
            [table.df for table in camelot.read_pdf(HACKATHON_DATA_PATH, pages=pages, flavor="stream")]
        )

    def get_summary_data(self, pages: str, filename: str):
        tables: List[pd.DataFrame] = []
        for table in self.fetch(pages):
            start_idx = table.index[table.iloc[:, 0] == "Rank"].values[0]
            tables.append(table[start_idx + 1:])
        data = pd.concat(tables)
        data.columns = self.SUMMARY_COLUMNS
        data = format_data(data)
        persist_data(data=data, filename=filename)
        data["rank_delta_2020"] = data["rank_delta_2020"].str.replace("–", "-")
        data["score_delta_2020"] = data["score_delta_2020"].str.replace("–", "-")
        return data

    def get_city_data(self, pages: str):
        """get data from city info table"""
        tables: List[pd.DataFrame] = []
        for table in self.fetch(pages):
            if pages == "158-160" and "Table E2." in table.iloc[0][0]:
                continue
            start_idx = table.index[table.iloc[:, 0] == "City"].values[0]
            tables.append(table[start_idx +1:])
        data = pd.concat(tables)
        data.columns = self.CITY_INFO_COLUMNS
        data = format_data(data)
        data["msa_population"] = pd.to_numeric(data["msa_population"].str.replace(",", ""))
        data["10yr_avg_annual_population_change"] = data["10yr_avg_annual_population_change"].str.replace("%", "").str.replace("–", "-").astype(float) / 100
        return data

    def get_state_data():
        return pd.read_csv("https://database.aceee.org/aceee_state_download.csv", encoding = "ISO-8859-1")

def persist_data(data: pd.DataFrame, filename: str):
    data.to_csv(f"/Users/sean/Documents/can-scrapers/can_tools/scrapers/hackathon/data/{filename}", index=False)

def format_data(data):
    data = (
        data.loc[~data["city"].isin(LOCATIONS_TO_DROP)]
        .replace(LOCATIONS_TO_RENAME)
        .assign(
            city=lambda row: row["city"].str.lower().str.replace(" ", "-").str.replace(".", "").str.replace(",", ""),
        )
        .replace("washington-dc", "washington")
    )
    # See TODO above
    return data.loc[:, [col for col in data.columns if col != "drop"]]