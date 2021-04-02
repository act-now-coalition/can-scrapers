import datetime as dt
import json

import pandas as pd
import requests
import us
from bs4 import BeautifulSoup

from can_tools.scrapers.base import ScraperVariable
from can_tools.scrapers.official.base import StateDashboard


class DelawareStateVaccine(StateDashboard):
    """
    Fetch state level covid data from Delaware's database
    """

    # Initialize
    source = "https://myhealthycommunity.dhss.delaware.gov"
    source_name = "Delaware Health and Social Services"
    has_location = True
    location_type = "state"
    state_fips = int(us.states.lookup("Delaware").fips)

    def fetch(self):
        # Mini-breakout for vaccine data, currently stored in embedded html
        # Recommend waiting till they have robust vaccine reporting before
        #   porting this into some sort of more robust parent class
        url = "https://myhealthycommunity.dhss.delaware.gov/locations/state/covid19_vaccine_administrations"
        r = requests.get(url)
        suppe = BeautifulSoup(r.text, features="lxml")

        # New Vaccines administered
        tdata = json.loads(
            suppe.find(
                "div",
                {"aria-labelledby": "chart-covid-vaccine-administrations-daily-label"},
            )["data-charts--covid-vaccine-administrations-daily-config-value"]
        )

        sd = tdata["startDate"]
        startDate = dt.datetime(sd[0], sd[1], sd[2])
        for srs in tdata["series"]:
            if srs["name"] == "Daily Count":
                data = srs["data"]
                idx = pd.date_range(startDate, periods=len(data), freq="d")
                df1 = pd.DataFrame(
                    data=data, columns=["NewVaccineAdminstrd"], index=idx
                )

        # Cumulative vaccines administered
        tdata2 = json.loads(
            suppe.find("div", {"aria-labelledby": "chart-line-chart-label"})[
                "data-charts--line-chart-config-value"
            ]
        )

        sd = tdata2["startDate"]
        startDate = dt.datetime(sd[0], sd[1], sd[2])
        for srs in tdata2["series"]:
            if srs["name"] == "State of Delaware":
                data = srs["data"]
                idx = pd.date_range(startDate, periods=len(data), freq="d")
                df2 = pd.DataFrame(
                    data=data, columns=["CumVaccineAdminstrd"], index=idx
                )

        # Vaccine deliveries
        url = "https://myhealthycommunity.dhss.delaware.gov/locations/state/covid19_vaccine_deliveries"
        r = requests.get(url)
        suppe = BeautifulSoup(r.text, features="lxml")

        tdata = json.loads(
            suppe.find("div", {"aria-labelledby": "chart-line-chart-label"})[
                "data-charts--line-chart-config-value"
            ]
        )

        sd = tdata["startDate"]
        startDate = dt.datetime(sd[0], sd[1], sd[2])
        for srs in tdata["series"]:
            if srs["name"] == "State of Delaware":
                data = srs["data"]
                idx = pd.date_range(startDate, periods=len(data), freq="d")
                df3 = pd.DataFrame(data=data, columns=["NewVaccineDelivrd"], index=idx)
                df3["CumVaccineDelivrd"] = df3["NewVaccineDelivrd"].cumsum()

        # End of vaccine data grab
        df = (
            df1.merge(df2, left_index=True, right_index=True)
            .merge(df3, left_index=True, right_index=True)
            .assign(location=self.state_fips)
        )
        df.index = df.index.set_names("dt")
        df = df.reset_index()

        return df

    def normalize(self, data):
        df = data
        crename = {
            "NewVaccineAdminstrd": ScraperVariable(
                category="total_vaccine_doses_administered",
                measurement="new",
                unit="doses",
            ),
            "CumVaccineAdminstrd": ScraperVariable(
                category="total_vaccine_doses_administered",
                measurement="cumulative",
                unit="doses",
            ),
            "CumVaccineDelivrd": ScraperVariable(
                category="total_vaccine_distributed",
                measurement="cumulative",
                unit="doses",
            ),
        }

        out = (
            df.melt(id_vars=["location", "dt"], value_vars=crename.keys())
            .assign(vintage=self._retrieve_vintage())
            .dropna()
        )
        out.loc[:, "value"] = pd.to_numeric(out["value"])

        # Extract category information and add other variable context
        out = self.extract_scraper_variables(out, crename)

        cols_to_keep = [
            "vintage",
            "dt",
            "location",
            "category",
            "measurement",
            "unit",
            "age",
            "race",
            "ethnicity",
            "sex",
            "value",
        ]
        return out.loc[:, cols_to_keep]
