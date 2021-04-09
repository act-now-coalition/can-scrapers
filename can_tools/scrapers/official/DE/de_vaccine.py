import datetime as dt
import json

import pandas as pd
import requests
import us
from bs4 import BeautifulSoup

from can_tools.scrapers import variables
from can_tools.scrapers.base import CMU
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


class DelawareCountyVaccine(DelawareStateVaccine):
    kent_fully_vaccinated_url = "https://myhealthycommunity.dhss.delaware.gov/locations/county-kent/covid19_vaccine_fully_vaccinated"
    new_castle_fully_vaccinated_url = "https://myhealthycommunity.dhss.delaware.gov/locations/county-new-castle/covid19_vaccine_fully_vaccinated"
    sussex_fully_vaccinated_url = "https://myhealthycommunity.dhss.delaware.gov/locations/county-sussex/covid19_vaccine_fully_vaccinated"
    kent_total_url = "https://myhealthycommunity.dhss.delaware.gov/locations/county-kent/covid19_vaccine_administrations"
    new_castle_total_url = "https://myhealthycommunity.dhss.delaware.gov/locations/county-new-castle/covid19_vaccine_administrations"
    sussex_total_url = "https://myhealthycommunity.dhss.delaware.gov/locations/county-sussex/covid19_vaccine_administrations"

    has_location = False
    location_type = "county"

    variables = {
        "FirstDose": CMU(
            category="total_vaccine_initiated", measurement="new", unit="doses"
        ),
        "SecondDose": CMU(
            category="total_vaccine_completed", measurement="new", unit="doses"
        ),
        "TotalDoses": CMU(
            category="total_vaccine_doses_administered", measurement="new", unit="doses"
        ),
    }

    def _fetch_fully_vaccinated(self):
        dfs = []
        fully_vaccinated_urls = [
            {"county": "Kent", "url": self.kent_fully_vaccinated_url},
            {"county": "New Castle", "url": self.new_castle_fully_vaccinated_url},
            {"county": "Sussex", "url": self.sussex_fully_vaccinated_url},
        ]
        for curl in fully_vaccinated_urls:
            county = curl["county"]
            url = curl["url"]
            r = requests.get(url)
            soup = BeautifulSoup(r.text, features="lxml")
            tdata = json.loads(
                soup.find(
                    "div",
                    {"aria-labelledby": "chart-covid-vaccine-fully-vaccinated-label"},
                )["data-charts--covid-vaccine-fully-vaccinated-config-value"]
            )
            sd = tdata["startDate"]
            # Parse start date
            startDate = dt.datetime(sd[0], sd[1], sd[2])
            # Get first dose data
            first_dose_data = tdata["series"][0]["data"]
            idx = pd.date_range(startDate, periods=len(first_dose_data), freq="d")
            first_dose_df = pd.DataFrame(
                data=first_dose_data, columns=["FirstDose"], index=idx
            )
            # Get second dose data
            second_dose_data = tdata["series"][1]["data"]
            idx = pd.date_range(startDate, periods=len(second_dose_data), freq="d")
            second_dose_df = pd.DataFrame(
                data=second_dose_data, columns=["SecondDose"], index=idx
            )
            df = first_dose_df.join(second_dose_df)
            df["location_name"] = county
            dfs.append(df)
        return pd.concat(dfs)

    def _fetch_total_administered(self):
        dfs = []
        total_urls = [
            {"county": "Kent", "url": self.kent_total_url},
            {"county": "New Castle", "url": self.new_castle_total_url},
            {"county": "Sussex", "url": self.sussex_total_url},
        ]
        for curl in total_urls:
            county = curl["county"]
            url = curl["url"]
            r = requests.get(url)
            soup = BeautifulSoup(r.text, features="lxml")
            tdata = json.loads(
                soup.find(
                    "div",
                    {
                        "aria-labelledby": "chart-covid-vaccine-administrations-daily-label"
                    },
                )["data-charts--covid-vaccine-administrations-daily-config-value"]
            )
            sd = tdata["startDate"]
            # Parse start date
            startDate = dt.datetime(sd[0], sd[1], sd[2])
            # Get first dose data
            total_df = None
            for srs in tdata["series"]:
                if srs["name"] == "Daily Count":
                    total_data = srs["data"]
                    idx = pd.date_range(startDate, periods=len(total_data), freq="d")
                    total_df = pd.DataFrame(
                        data=total_data, columns=["TotalDoses"], index=idx
                    )
            if total_df is None:
                raise "Couln't get county total data"
            df = total_df
            df["location_name"] = county
            dfs.append(df)
        return pd.concat(dfs)

    def fetch(self):
        totals = self._fetch_total_administered()
        doses = self._fetch_fully_vaccinated()
        return {"totals": totals, "doses": doses}

    def normalize(self, data):

        totals = data["totals"]
        totals = totals.reset_index().rename(columns={"index": "dt"})
        doses = data["doses"]
        doses = doses.reset_index().rename(columns={"index": "dt"})
        df = (
            totals.set_index(["dt", "location_name"])
            .join(doses.set_index(["dt", "location_name"]))
            .reset_index()
        )

        df = df.fillna(0)
        out = self._reshape_variables(df, self.variables)

        return out
