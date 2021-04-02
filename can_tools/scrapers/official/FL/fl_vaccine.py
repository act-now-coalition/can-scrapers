import camelot
import pandas as pd
import requests
import us

from can_tools.scrapers import ScraperVariable
from can_tools.scrapers.official.base import StateDashboard


class FloridaCountyVaccine(StateDashboard):
    has_location = False
    source = "https://floridahealthcovid19.gov/#latest-stats"
    location_type = "county"
    state_fips = int(us.states.lookup("Florida").fips)
    fetch_url = "http://ww11.doh.state.fl.us/comm/_partners/covid19_report_archive/vaccine/vaccine_report_latest.pdf"
    source_name = "Florida Department of Health"

    def fetch(self):
        return camelot.read_pdf(self.fetch_url, pages="2-end", flavor="stream")

    def normalize(self, data):
        # read in data, remove extra header cols, rename column names
        dfs = []
        for el in data:
            dfs.append(self._truncate_data(el.df))
        df = pd.concat(dfs)

        # # Ignore data from unknown region (no fips code) and fix naming convention for problem counties, and total state vals
        df = df.query(
            "location_name != 'Unknown' &"
            "location_name != 'Out-Of-State' &"
            "location_name != 'Total'"
        )
        df = df.replace({"location_name": {"Desoto": "DeSoto", "Dade": "Miami-Dade"}})

        # Make all columns (except location) numeric
        for col in df.columns:
            if col == "location_name":
                continue
            else:
                df.loc[:, col] = pd.to_numeric(df.loc[:, col].str.replace(",", ""))

        # First dose and second dose need to be added together to get at least one vaccinated
        df.loc[:, "first_dose_total"] = df.eval(
            "first_dose_total + series_complete_total"
        )

        crename = {
            "first_dose_new": ScraperVariable(
                category="total_vaccine_initiated",
                measurement="new",
                unit="people",
            ),
            "series_complete_new": ScraperVariable(
                category="total_vaccine_completed",
                measurement="new",
                unit="people",
            ),
            "total_people_vaccinated_new": ScraperVariable(
                category="total_vaccine_doses_administered",
                measurement="new",
                unit="doses",
            ),
            "first_dose_total": ScraperVariable(
                category="total_vaccine_initiated",
                measurement="cumulative",
                unit="people",
            ),
            "series_complete_total": ScraperVariable(
                category="total_vaccine_completed",
                measurement="cumulative",
                unit="people",
            ),
            "total_people_vaccinated_total": ScraperVariable(
                category="total_vaccine_doses_administered",
                measurement="cumulative",
                unit="doses",
            ),
        }

        out = df.melt(id_vars=["location_name"], value_vars=crename.keys()).dropna()
        out = self.extract_scraper_variables(out, crename)
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

    def _truncate_data(self, data):
        """
        fix the column names and remove all the gibberesh data before the first county/real entry in the table.
        **this method feels like a band-aid fix, but it has worked for the past two weeks and im not sure of a better way**
        """
        data.columns = [
            "location_name",
            "first_dose_new",
            "series_complete_new",
            "total_people_vaccinated_new",
            "first_dose_total",
            "series_complete_total",
            "total_people_vaccinated_total",
        ]

        # the data/table starts two lines after 'County of residence' appears in the location column
        return data[data.query("location_name == 'County of residence'").index[0] + 2 :]
