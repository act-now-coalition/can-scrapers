import requests
import us
import re
import pandas as pd
import json
from bs4 import BeautifulSoup as bs
from typing import Dict, Tuple
from can_tools.scrapers.official.base import CountyDashboard
from can_tools.scrapers import variables


class ArizonaMaricopaVaccine(CountyDashboard):
    has_location = False
    location_type = "county"
    state_fips = int(us.states.lookup("Arizona").fips)
    source = "https://www.maricopa.gov/5671/Public-Vaccine-Data"
    source_name = "ASIIS"
    variables = {
        "total_doses_administered": variables.TOTAL_DOSES_ADMINISTERED_ALL,
        "total_vaccine_completed": variables.FULLY_VACCINATED_ALL,
        "total_vaccine_initiated": variables.INITIATING_VACCINATIONS_ALL,
    }

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
        raw_data = soup.find_all("script")[1]
        raw_data = str(raw_data).replace("\\", "")

        # extract relevent json from string
        raw_json = (
            "{"
            + re.findall(r"\"data\":\{.*?\]", raw_data, flags=re.MULTILINE)[1]
            + "}}"
        )

        # get only relevent data from this JSON
        return json.loads(raw_json)["data"]["changes"]

    def fetch(self) -> Tuple[requests.models.Response]:
        # data are stored in two different cards, so fetch both
        url = "https://datawrapper.dwcdn.net/Y9bAu/3/"
        initiated_url = "https://datawrapper.dwcdn.net/ECVzt/7/"
        return requests.get(url), requests.get(initiated_url)

    def normalize(self, data) -> pd.DataFrame:
        # get the real urls of both cards
        url = self._get_url(data[0])
        init_url = self._get_url(data[1])

        # extract the json/data from both cards
        records = self._get_json(url)
        records_init = self._get_json(init_url)

        # only keep records that correspond to a data definition (vaccine initiated, doses admin, etc..)
        records = [r for r in records if r["column"] == 1 and r["row"] in [1, 3]]
        records_init = [r for r in records_init if r["column"] == 1 and r["row"] == 1]

        # rename initiated values, as both dicts have "1" as a row value
        for r in records_init:
            r["row"] = "total_vaccine_initiated"

        # dump rows into df
        rows = records + records_init
        df = pd.DataFrame.from_records(rows)
        df = df.assign(
            dt=pd.to_datetime(df["time"], unit="ms").dt.date,
            value=df["value"].str.replace("r", "").astype(int),
            variable=df["row"].replace(
                {
                    1: "total_doses_administered",
                    3: "total_vaccine_completed",
                }
            ),
            location_name="Maricopa",
            vintage=self._retrieve_vintage(),
        ).drop(columns={"row", "column", "time", "ignored", "previous"})

        out = self.extract_CMU(df, self.variables)

        # filter results from before march (bad data)
        out = out[out["dt"] > pd.to_datetime("2021-3-01")]
        return out.drop(columns={"variable"})
