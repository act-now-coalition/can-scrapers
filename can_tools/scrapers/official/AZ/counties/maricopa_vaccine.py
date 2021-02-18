import pandas as pd
import requests
import us

from bs4 import BeautifulSoup
from can_tools.scrapers.base import CMU
from can_tools.scrapers.official.base import CountyDashboard


class ArizonaMaricopaVaccine(CountyDashboard):
    """
    Fetch county level Covid-19 vaccination data from official Maricopa county website
    """

    source = "https://www.maricopa.gov/5641/COVID-19-Vaccine"
    source_name = "Maricopa County"
    has_location = False
    location_type = "county"
    state_fips = int(us.states.lookup("Arizona").fips)

    def fetch(self):
        # Set url of website
        url = "https://www.maricopa.gov/5641/COVID-19-Vaccine"
        request = requests.get(url)

        if not request.ok:
            message = f"Could not request data from {url}"
            raise ValueError(message)

        return request.content

    def normalize(self, data) -> pd.DataFrame:
        # Read data into Beautiful Soup
        bs = BeautifulSoup(data, "html.parser")

        # Find the doses given
        doses = bs.find_all("h2", class_="dataNumber")[1::1][0].text.replace(",", "")

        # Create data frame
        df = pd.DataFrame(
            {
                "location_name": ["Maricopa"],
                "total_vaccine_doses_administered": pd.to_numeric(doses),
            }
        )

        # Create dictionary for columns to map
        crename = {
            "total_vaccine_doses_administered": CMU(
                category="total_vaccine_doses_administered",
                measurement="cumulative",
                unit="doses",
            ),
        }

        # Move things into long format
        df = df.melt(id_vars=["location_name"], value_vars=crename.keys()).dropna()

        # Determine the category of each observation
        out = self.extract_CMU(df, crename)

        # Add rows that don't change
        out["vintage"] = self._retrieve_vintage()
        out["dt"] = self._retrieve_dt("US/Arizona")

        return out

    def validate(self, df, df_hist) -> bool:
        "A pinch of probability is worth a pound of perhaps. - James Thurber"
        return True
