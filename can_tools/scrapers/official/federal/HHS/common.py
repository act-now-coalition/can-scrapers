import bs4 as bs
import requests

from io import BytesIO
from typing import Dict

import pandas as pd
import us

from can_tools.scrapers.official.base import FederalDashboard
from can_tools.scrapers.base import CMU, DatasetBase


class HHSDataset(FederalDashboard):
    source: str

    def search_homepage_for_download(self, ds_name):
        # Download page html and turn into soup
        res = requests.get(self.source)
        soup = bs.BeautifulSoup(res.text, features="lxml")

        # Filter through links to find all download links
        dls = [x for x in soup.find_all(name="a") if x.text.strip() == "Download"]
        filtered_dls = list(filter(lambda x: ds_name in x["href"], dls))

        # Make sure it has length 1... Otherwise we're in trouble
        assert len(filtered_dls) == 1
        dl = filtered_dls[0]["href"]

        # Download content at the site
        dl_res = requests.get(dl)

        # Encode as utf-8
        data = dl_res.text.encode("utf-8")

        return data

    def normalize_helper(self, data: str, crename: Dict[str, CMU]) -> pd.DataFrame:
        "Helper function for repeated operations in data reading"
        # Read the dataframe from the string csv
        df = pd.read_csv(BytesIO(data))
        df.columns = [x.lower().strip() for x in df.columns]

        # TODO: wtf is CW an abbreviation for?
        df = df.query("state != 'CW'")

        # Set date and fips code
        df.loc[:, "dt"] = pd.to_datetime(df["collection_date"])
        df.loc[:, "location"] = df["state"].map(lambda x: int(us.states.lookup(x).fips))

        # Put into long form
        out = df.melt(id_vars=["dt", "location"], value_vars=crename.keys())
        out.loc[:, "value"] = pd.to_numeric(
            out["value"].astype(str).str.replace(",", "")
        )

        # Add category, measurement, unit, age, sex, race
        out = self.extract_CMU(out, crename)
        out["vintage"] = self._retrieve_vintage()
        cols_2_keep = [
            "vintage",
            "dt",
            "location",
            "category",
            "measurement",
            "unit",
            "age",
            "sex",
            "race",
            "value",
        ]

        return out.loc[:, cols_2_keep]
