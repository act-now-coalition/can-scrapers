from typing import Any

import pandas as pd
import us

from can_tools.scrapers.base import CMU, DatasetBase
from can_tools.scrapers.official.base import SODA


class CTCountyVaccine(SODA, DatasetBase):

    baseurl = "https://data.ct.gov/"
    resource_id = "5g42-tpzq"
    has_location = False
    location_type = "county"
    state_fips = int(us.states.lookup("Connecticut").fips)
    source = "https://data.ct.gov/resource/5g42-tpzq.json"
    source_name = "Official Connecticut State Government Website"

    def fetch(self):
        dataset = self.get_dataset(self.resource_id)
        return dataset

    def normalize(self, data: Any) -> pd.DataFrame:
        data["date"] = pd.to_datetime(data["date"])
        data = data.rename(
            columns={"date": "dt", "county_of_residence": "location_name"}
        )
        unwanted_loc = ["Total", "Address pending validation", "Residence out of state"]
        data = data.query("location_name not in @unwanted_loc")

        crename = {
            "fully_vaccinated": CMU(
                category="total_vaccine_completed",
                measurement="cumulative",
                unit="people",
            ),
            "initiated_vaccination": CMU(
                category="total_vaccine_initiated",
                measurement="cumulative",
                unit="people",
            ),
        }
        out = data.melt(
            id_vars=["dt", "location_name"], value_vars=crename.keys()
        ).dropna()
        out["value"] = pd.to_numeric(out.loc[:, "value"])
        out["vintage"] = self._retrieve_vintage()
        out = self.extract_CMU(out, crename)
        return out
