import pandas as pd
import camelot
import requests
import us

from can_tools.scrapers import CMU
from can_tools.scrapers.official.base import StateDashboard


class FloridaCountyVaccine(StateDashboard):
    has_location = False
    source = "https://floridahealthcovid19.gov/#latest-stats"
    location_type = "county"
    state_fips = int(us.states.lookup("Florida").fips)
    fetch_url = "http://ww11.doh.state.fl.us/comm/_partners/covid19_report_archive/vaccine/vaccine_report_latest.pdf"

    def fetch(self):
        return camelot.read_pdf(self.fetch_url, pages="2", flavor="stream")

    def normalize(self, data):
        # read in data, remove extra header cols, rename column names
        if len(data) > 1:
            raise ValueError("more tables returned than expected value")

        df = data[0].df
        df = df.iloc[6:].reset_index(drop=True)
        df.columns = [
            "location_name",
            "first_dose_new",
            "series_complete_new",
            "total_people_vaccinated_new",
            "first_dose_total",
            "series_complete_total",
            "total_people_vaccinated_total",
        ]

        # # Ignore data from unknown region (no fips code) and fix naming convention for problem counties, and total state vals
        df = df.query(
            "location_name != 'Unknown' &"
            "location_name != 'Out-Of-State' &"
            "location_name != 'Total'"
        )
        df = df.replace({"location_name": {"Desoto": "DeSoto", "Dade": "Miami-Dade"}})

        crename = {
            "first_dose_new": CMU(
                category="total_vaccine_initiated",
                measurement="new",
                unit="people",
            ),
            "series_complete_new": CMU(
                category="total_vaccine_completed",
                measurement="new",
                unit="people",
            ),
            "total_people_vaccinated_new": CMU(
                category="total_vaccine_doses_administered",
                measurement="new",
                unit="doses",
            ),
            "first_dose_total": CMU(
                category="total_vaccine_initiated",
                measurement="cumulative",
                unit="people",
            ),
            "series_complete_total": CMU(
                category="total_vaccine_completed",
                measurement="cumulative",
                unit="people",
            ),
            "total_people_vaccinated_total": CMU(
                category="total_vaccine_doses_administered",
                measurement="cumulative",
                unit="doses",
            ),
        }

        out = df.melt(id_vars=["location_name"], value_vars=crename.keys()).dropna()
        out = self.extract_CMU(out, crename)
        out.loc[:, "value"] = pd.to_numeric(out["value"].str.replace(",", ""))
        out["vintage"] = self._retrieve_vintage()
        out["dt"] = self._get_date()

        cols_to_keep = [
            "vintage",
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
        return out.loc[:, cols_to_keep]

    def _get_date(self):
        """
        retrieve the date that the PDF was last updated minus one day, return as date.
        if connection to source cannot be made, use yesterday's date.
        """
        res = requests.get(self.fetch_url)
        # if the connection fails, use yesterday's date as date
        if not res.ok:
            dt = self._retrieve_dtm1d("US/Eastern")
        else:
            dt = pd.to_datetime(
                res.headers["Last-Modified"], format="%a, %d %b %Y %H:%M:%S GMT"
            ) - pd.Timedelta(days=1)
        return dt.date()
