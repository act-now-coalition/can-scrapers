from can_tools.scrapers.base import CMU
from typing import List
import us
import requests
import pandas as pd
import bs4

from can_tools.scrapers.official.base import CountyDashboard


class LACaliforniaCountyVaccine(CountyDashboard):
    provider = "county"
    has_location = True
    location_type = "county"
    location = 6037
    state_fips = int(us.states.lookup("California").fips)
    source = "http://publichealth.lacounty.gov/media/Coronavirus/vaccine/vaccine-dashboard.htm"
    source_name = "County of Los Angeles Public Health"

    def fetch(self) -> str:
        res = requests.get(self.source)
        if res.ok:
            return res.content

        else:
            raise ValueError("Unable to access LA County Vaccine Dashboard")

    def normalize(self, data: str) -> pd.DataFrame:
        soup = bs4.BeautifulSoup(data, features="lxml")
        texts: List[str] = [x.text.lower() for x in soup.select(".bg-vaccineblue p")]
        assert len(texts) == 4, "expected 4 paragraphs in vaccine box"
        assert "doses administered" in texts[0]
        dt = pd.to_datetime(texts[1], format=f"as of %m/%d/%Y")
        total_dose = int(texts[2].replace(",", ""))

        prefix = "second doses:"
        assert texts[3].startswith(prefix)
        dose2 = int(texts[3][len(prefix) :].strip().replace(",", ""))

        df = pd.DataFrame(
            {"variable": ["total_dose", "dose2"], "value": [total_dose, dose2]}
        ).assign(dt=dt, vintage=self._retrieve_vintage(), location=self.location)

        cmus = {
            "total_dose": CMU(
                category="total_vaccine_doses_administered",
                measurement="cumulative",
                unit="doses",
            ),
            "dose2": CMU(
                category="total_vaccine_completed",
                measurement="cumulative",
                unit="people",
            ),
        }

        return df.pipe(self.extract_CMU, cmu=cmus).drop(["variable"], axis="columns")
