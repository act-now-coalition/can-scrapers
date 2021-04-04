import re
import tempfile
import pandas as pd
import camelot
import requests
import us
import textract

from can_tools.scrapers import CMU
from can_tools.scrapers.official.base import StateDashboard


class FloridaCountyVaccine(StateDashboard):
    has_location = False
    source = "https://floridahealthcovid19.gov/#latest-stats"
    location_type = "county"
    state_fips = int(us.states.lookup("Florida").fips)
    fetch_url = "http://ww11.doh.state.fl.us/comm/_partners/covid19_report_archive/vaccine/vaccine_report_latest.pdf"
    fetch_url_for_counties = "http://ww11.doh.state.fl.us/comm/_partners/covid19_report_archive/vaccine-county" \
                             "/vaccine_county_report_latest.pdf"

    source_name = "Florida Department of Health"

    def fetch(self):
        vaccine_data = camelot.read_pdf(self.fetch_url, pages="2-end", flavor="stream")

        county_names = []
        results = requests.get(self.fetch_url_for_counties)
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_file = '{}/vaccine_county_report_latest.pdf'.format(tmp_dir)
            with open(tmp_file, 'wb') as f:
                f.write(results.content)
                pdf_pages_headers = textract.process(tmp_file)
                county_names = re.findall(r"COVID-19: (?P<countyName>.*?) vaccine summary",
                                          pdf_pages_headers.decode("utf-8"))

        county_demographics_data = camelot.read_pdf(self.fetch_url_for_counties, pages="1-3", flavor="stream", row_tol=10)
        return {"vaccine_data": vaccine_data, "county_demographics_data": county_demographics_data, "county_names": county_names}

    def normalize(self, data):
        # read in data, remove extra header cols, rename column names
        dfs = []
        if "vaccine_data" in data:
            for el in data["vaccine_data"]:
                dfs.append(self._truncate_vaccine_data(el.df))
        df = pd.concat(dfs)

        dfs_demographics = []
        if "county_demographics_data" in data:
            for i in range(len(data["county_demographics_data"])):
                dfs_demographics.append(self._truncate_demographics_data(data["county_demographics_data"][i].df, data["county_names"][i]))
        df_demographics =pd.concat(dfs_demographics)

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

        age_replace = {
            "16-24 years": "16-24",
            "25-34 years": "25-34",
            "35-44 years": "35-44",
            "45-54 years": "45-54",
            "55-64 years": "55-64",
            "65-74 years": "65-74",
            "75-84 years": "75-84",
            "85+ years": "85_plus",
            "Age Unknown": "unknown",
        }
        age_df = self.normalize_group(df, "Age Groups", "age", age_replace, crename)

        gender_replace = {
            "Female": "female",
            "Male": "male",
            "Gender Unknown": "unknown",
        }
        gender_df = self.normalize_group(df, "Gender", "sex", gender_replace, crename)

        race_replace = {
            "American Indian or Alaska Native": "ai_an",
            "Asian": "asian",
            "Black or African-American": "black",
            "Native Hawaiian or Other Pacific Islander": "pacific_islander",
            "Other Race": "other",
            "White": "white",
        }
        race_col = [
            c for c in df["Category"].unique() if c is not None and "race" in c.lower()
        ][0]
        race_df = self.normalize_group(df, race_col, "race", race_replace, crename)

        ethnicity_replace = {"Hispanic or Latino": "hispanic"}
        eth_col = [
            c
            for c in df["Category"].unique()
            if c is not None and "ethnicity" in c.lower()
        ][0]
        eth_df = self.normalize_group(
            df, eth_col, "ethnicity", ethnicity_replace, crename
        )

        out = df.melt(id_vars=["location_name"], value_vars=crename.keys()).dropna()
        out = self.extract_CMU(out, crename)
        out["vintage"] = self._retrieve_vintage()
        out["dt"] = self._get_date()

        out: pd.DataFrame = pd.concat(
            [age_df, gender_df, race_df, eth_df, total_df], axis=0, ignore_index=True
        ).dropna()

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

    def _truncate_vaccine_data(self, data):
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

    def _truncate_demographics_data(self, data, county_name):

        # data.drop(data.columns[[0]], axis=1, inplace=True)

        data.columns = [
            "location_name",
            "age_group",
            "first_dose_total",
            "series_complete_total",
            "total_people_vaccinated_total",
        ]

        data.assign(location_name=county_name)

        # the data/table starts two lines after 'County of residence' appears in the location column
        startIndex = data.query("age_group == 'Age group'").index[0] + 1

        return data[startIndex: startIndex+8]