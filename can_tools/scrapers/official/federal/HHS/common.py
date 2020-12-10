import requests

from typing import Dict

import pandas as pd

from can_tools.scrapers.official.base import FederalDashboard


class HHSDataset(FederalDashboard):
    source: str
    dsid: str

    def dataset_details(self, decoder="utf-8-sig"):
        # Download page html and turn into soup
        dsid = self.dsid
        url = f"https://healthdata.gov/api/3/action/package_show?id={dsid}"
        res = requests.get(url)

        # Get the data link
        data_url = res.json()["result"][0]["resources"][0]["url"]

        # Download content at the site
        dl_res = requests.get(data_url)

        return dl_res.content.decode(decoder)
