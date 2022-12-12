import requests
import pandas as pd
from can_tools.scrapers import variables

import requests
import us
import re
import pandas as pd
import json
from bs4 import BeautifulSoup as bs
from can_tools.scrapers.official.base import CountyDashboard
from can_tools.scrapers import variables


class MaricopaVaccineRace(CountyDashboard):
    has_location = False
    location_type = "county"
    state_fips = int(us.states.lookup("Arizona").fips)
    source = "https://www.maricopa.gov/5671/Public-Vaccine-Data"
    source_name = "ASIIS"
    variables = {
        1: variables.INITIATING_VACCINATIONS_ALL,
    }
    demographic_data = True

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

    def _get_url(self, src_data: requests.models.Response) -> str:
        """
        the url from the source page automatically redirects to the actual card.
        this gets the url of the actual dashboard from the redirect.
        """

        soup = bs(src_data.text, "lxml")
        redirect = soup.find("meta")
        url = re.findall(r"https://.*", redirect["content"])[0]
        return url

    def _get_json(self, url: str) -> list:
        """
        extract the json containing the data from a script on the card
        """
        page = requests.get(url)
        soup = bs(page.text, "lxml")

        # extract the and format script that contains the data/JSON
        raw_data = soup.find_all("script")

        # The page may need to redirect multiple times before landing on the
        # card with the actual data. Check if a redirect is necessary by checking
        # the meta tag, and if so, re-call the function until the data is found.
        refresh_tag = soup.find_all("meta", attrs={"http-equiv": "REFRESH"})
        if refresh_tag:
            new_url = self._get_url(page)
            return self._get_json(new_url)

        raw_data = str(raw_data[1]).replace("\\", "")
        # extract relevent json from string
        raw_json = (
            "{"
            + re.findall(r"\"data\":\{\"changes.*?\]", raw_data, flags=re.MULTILINE)[0]
            + "}}"
        )
        # get only relevent data from this JSON
        return json.loads(raw_json)["data"]["changes"]
