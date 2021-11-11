import requests
import pandas as pd
from can_tools.scrapers import variables
from can_tools.scrapers import ArizonaMaricopaVaccine


class MaricopaVaccineRace(ArizonaMaricopaVaccine):
    variables = {
        1: variables.INITIATING_VACCINATIONS_ALL,
    }

    def fetch(self) -> requests.models.Response:
        url = "https://datawrapper.dwcdn.net/T99SS/4/"
        return requests.get(url)

    def normalize(self, data) -> pd.DataFrame:
        url = self._get_url(data)

        # extract the json/data from the cards
        records = self._get_json(url)
        return (
            pd.DataFrame.from_records(records)
            .query("column == 1 and row != 0")
            .assign(
                race=lambda row: row["row"].replace(
                    {
                        1: "all",
                        2: "ai_an",
                        3: "asian_or_pacific_islander",
                        4: "black",
                        5: "other",
                        6: "unknown",
                        7: "white",
                    }
                ),
                location_name="Maricopa",
                dt=lambda row: pd.to_datetime(row["time"], unit="ms").dt.date,
                vintage=self._retrieve_vintage(),
                value=lambda row: pd.to_numeric(row["value"]),
            )
            .pipe(
                self.extract_CMU,
                var_name="column",
                cmu=self.variables,
                skip_columns=["race"],
            )
        )
