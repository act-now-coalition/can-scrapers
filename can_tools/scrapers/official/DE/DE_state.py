import us
import requests
import json
import pandas as pd
from bs4 import BeautifulSoup

from can_tools.scrapers.base import CMU
from can_tools.scrapers.official.base import StateDashboard


class DelawareStateData(StateDashboard):
    """
    Fetch state level covid data from Delaware's database
    """

    # Initialize
    source = "https://myhealthycommunity.dhss.delaware.gov/locations/state/download_covid_19_data"
    # source = 'https://myhealthycommunity.dhss.delaware.gov/locations/county-new-castle/download_covid_19_data'
    has_location = True
    location_type = "state"
    state_fips = int(us.states.lookup("Delaware").fips)

    def fetch(self):
        data = pd.read_csv(self.source)
        data["Date"] = pd.to_datetime(data[["Year", "Month", "Day"]])
        data = data[data["Date"] == data["Date"].max()]
        data = data[data["Age adjusted"] == False]
        data["StatUnit"] = data["Statistic"] + "_" + data["Unit"]
        data.drop(
            columns=[
                "Year",
                "Month",
                "Day",
                "Location",
                "Age adjusted",
                "Date used",
                "Statistic",
                "Unit",
                "County",
            ],
            inplace=True,
        )
        data["location"] = self.state_fips
        data.reset_index(inplace=True, drop=True)
        data = pd.pivot_table(
            data,
            values="Value",
            index=["location", "Date"],
            columns="StatUnit",
            aggfunc="first",
        ).reset_index()

        # Mini-breakout for vaccine data, currently stored in embedded html
        # Recommend waiting till they have robust vaccine reporting before
        #   porting this into some sort of more robust parent class

        url = "https://myhealthycommunity.dhss.delaware.gov/locations/state/covid19_vaccine_administrations"
        r = requests.get(url)
        suppe = BeautifulSoup(r.text, features="lxml")
        tdata = json.loads(
            suppe.find(
                "div",
                {"aria-labelledby": "chart-covid-vaccine-administrations-daily-label"},
            )["data-charts--covid-vaccine-administrations-daily-config-value"]
        )
        for srs in tdata["series"]:
            if srs["name"] == "Daily Count":
                data["NewVaccineAdminstrd"] = srs["data"][-1]
        tdata2 = json.loads(
            suppe.find("div", {"aria-labelledby": "chart-line-chart-label"})[
                "data-charts--line-chart-config-value"
            ]
        )
        for srs in tdata2["series"]:
            if srs["name"] == "State of Delaware":
                data["CumVaccineAdminstrd"] = srs["data"][-1]

        url = "https://myhealthycommunity.dhss.delaware.gov/locations/state/covid19_vaccine_deliveries"
        r = requests.get(url)
        suppe = BeautifulSoup(r.text, features="lxml")
        tdata = json.loads(
            suppe.find("div", {"aria-labelledby": "chart-line-chart-label"})[
                "data-charts--line-chart-config-value"
            ]
        )
        for srs in tdata["series"]:
            if srs["name"] == "State of Delaware":
                data["NewVaccineDelivrd"] = srs["data"][-1]
                data["CumVaccineDelivrd"] = sum(srs["data"])

        # End of vaccine data grab

        return data

    def normalize(self, data):
        df = data
        crename = {
            "New Positive Cases_people": CMU(
                category="cases", measurement="new", unit="people"
            ),
            "Deaths_people": CMU(
                category="deaths", measurement="cumulative", unit="people"
            ),
            "Total Tests_tests": CMU(
                category="unspecified_tests_total",
                measurement="cumulative",
                unit="specimens",
            ),
            "Positive Tests_tests": CMU(
                category="unspecified_tests_positive",
                measurement="cumulative",
                unit="specimens",
            ),
            "Cumulative Number of Positive Cases_people": CMU(
                category="cases", measurement="cumulative", unit="people"
            ),
            "Persons Tested Negative_people": CMU(
                category="unspecified_tests_negative",
                measurement="cumulative",
                unit="unique_people",
            ),
            "Total Persons Tested_people": CMU(
                category="unspecified_tests_total",
                measurement="cumulative",
                unit="unique_people",
            ),
            "NewVaccineAdminstrd": CMU(
                category="total_vaccine_doses_administered",
                measurement="new",
                unit="doses",
            ),
            "CumVaccineAdminstrd": CMU(
                category="total_vaccine_doses_administered",
                measurement="cumulative",
                unit="doses",
            ),
            "CumVaccineDelivrd": CMU(
                category="total_vaccine_allocated",
                measurement="cumulative",
                unit="doses",
            ),
        }

        out = (
            df.melt(id_vars=["location"], value_vars=crename.keys())
            .assign(
                dt=self._retrieve_dt("US/Eastern"), vintage=self._retrieve_vintage()
            )
            .dropna()
        )
        out.loc[:, "value"] = pd.to_numeric(out["value"])
        out.rename(columns={"StatUnit": "variable"}, inplace=True)
        # Extract category information and add other variable context
        out = self.extract_CMU(out, crename)

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
