import pandas as pd
import requests
import us

from can_tools.scrapers.base import CMU
from can_tools.scrapers.official.base import StateDashboard
from can_tools.scrapers.util import requests_retry_session


class NewMexicoBase(StateDashboard):
    state_fips = int(us.states.lookup("New Mexico").fips)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.session = requests_retry_session()


class NewMexicoVaccineCountyFirstDose(NewMexicoBase):
    """
    Fetch county level Covid-19 vaccination data from official state of New Mexico REST APIs
    """

    source = "https://cvvaccine.nmhealth.org/public-dashboard.html"
    has_location = False
    location_type = "county"

    def fetch(self) -> requests.Response:
        # Set url of downloadable dataset
        url = "https://cvvaccine.nmhealth.org/api/GetCounties"
        request = self.session.get(url)

        if not request.ok:
            message = f"Could not request data from {url}"
            raise ValueError(message)

        return request.json()

    def normalize(self, data) -> pd.DataFrame:
        # Read data into data frame
        key = "data"
        if key not in data:
            raise ValueError(f"Expected to find {key} in JSON response")
        df = pd.DataFrame(data[key])

        # Determine what columns to keep
        cols_to_keep = [
            "county",
            "date",
            "modernaShipped",
            "pfizerShipped",
            "dosesAdministered",
            "totalShipped",
            "percentVaccinated",
        ]

        # Drop extraneous columns
        df = df.loc[:, cols_to_keep]

        # Rename columns
        df = df.rename(columns={"date": "dt", "county": "location_name"})

        # Convert date time column to a datetime
        df = df.assign(dt=lambda x: pd.to_datetime(x["dt"]))

        # Create dictionary for columns to map
        crename = {
            "modernaShipped": CMU(
                category="moderna_vaccine_distributed",
                measurement="cumulative",
                unit="doses",
            ),
            "pfizerShipped": CMU(
                category="pfizer_vaccine_distributed",
                measurement="cumulative",
                unit="doses",
            ),
            "dosesAdministered": CMU(
                category="total_vaccine_doses_administered",
                measurement="cumulative",
                unit="doses",
            ),
            "totalShipped": CMU(
                category="total_vaccine_distributed",
                measurement="cumulative",
                unit="doses",
            ),
            "percentVaccinated": CMU(
                category="total_vaccine_initiated",
                measurement="current",
                unit="percentage",
            ),
        }

        # Move things into long format
        df = df.melt(
            id_vars=["dt", "location_name"], value_vars=crename.keys()
        ).dropna()

        # Determine the category of each observation
        df = self.extract_CMU(df, crename)

        # Determine what columns to keep
        cols_to_keep = [
            "dt",
            "location_name",
            "category",
            "measurement",
            "unit",
            "age",
            "race",
            "ethnicity",
            "sex",
            "value",
        ]

        # Drop extraneous columns
        out = df.loc[:, cols_to_keep]

        # Drop the information that we won't be keeping track of
        # loc_not_keep = ["Out of State", "Pending"]
        # out = out.query("location_name not in @loc_not_keep")

        # Fix incorrectly spelled county names
        # loc_replacer = {"Dekalb": "DeKalb"}
        # out = out.replace({"location_name": loc_replacer})

        # Convert value columns
        out["value"] = out["value"].astype(int)

        # Add rows that don't change
        out["vintage"] = self._retrieve_vintage()

        return out

    def validate(self, df, df_hist) -> bool:
        "Software never was perfect and won’t get perfect. But is that a license to create garbage? The missing ingredient is our reluctance to quantify quality. - Boris Beizer"
        return True
