import requests
import us
import re
import pandas as pd
import json
from bs4 import BeautifulSoup as bs
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
        "1_dose_pf_mod": variables.INITIATING_VACCINATIONS_ALL,
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

    def fetch(self) -> requests.models.Response:
        init_url = "https://datawrapper.dwcdn.net/Y9bAu/3/"
        return requests.get(init_url)

    def normalize(self, data) -> pd.DataFrame:
        # get the url of the dashboard
        url = self._get_url(data)

        # make request to the dashboard itself
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
        records = json.loads(raw_json)["data"]["changes"]
        records = [r for r in records if r["column"] == 1]

        # dump into df
        df = pd.DataFrame.from_records(records)
        df = (
            df.assign(
                dt=pd.to_datetime(df["time"], unit="ms").dt.date,
                value=df["value"].str.replace("r", "").astype(int),
                variable=df["row"].replace(
                    {
                        1: "total_doses_administered",
                        2: "1_dose_pf_mod",
                        3: "total_vaccine_completed",
                        4: "unknown_dose",
                    }
                ),
                location_name="Maricopa",
                vintage=self._retrieve_vintage(),
            )
            .drop(columns={"row", "column", "time", "ignored", "previous"})
            .query('variable not in ["unknown_dose", "1_dose_pf_mod"]')
        )

        out = self.extract_CMU(df, self.variables)
        
        # filter results from before march (bad data)
        out = out[out['dt'] > pd.to_datetime("2021-3-01")]
        return out.drop(columns={"variable"})
