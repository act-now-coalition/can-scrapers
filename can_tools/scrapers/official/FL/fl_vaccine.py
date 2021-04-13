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
                county_names = list(map(lambda x: x if not x.endswith(" County") else x[:-7], county_names))

        county_demographics_data = camelot.read_pdf(self.fetch_url_for_counties, pages="1-end", flavor="stream", row_tol=10)
        return {"vaccine_data": vaccine_data, "county_demographics_data": county_demographics_data, "county_names": county_names}

    def normalize_group(self, df, demographic, dgroup, group_rename, cmu):
        keep_vals = list(group_rename.keys())
        foo = df.query("category == @demographic &" "Join_Name in @keep_vals").rename(
            columns={"Join_Name": dgroup}
        )
        foo.loc[:, dgroup] = foo.loc[:, dgroup].str.strip()
        foo = foo.replace({dgroup: group_rename})

        # Keep all but current demographic info
        all_demographics = ["age", "race", "ethnicity", "sex"]
        all_demographics.remove(dgroup)
        cmu_cols = ["category", "measurement", "unit"] + all_demographics
        foo = self.extract_CMU(foo, cmu, columns=cmu_cols)

        # Drop Category/variable
        foo = foo.drop(["Category", "variable"], axis="columns")

    def normalize(self, data):
        # read in data, remove extra header cols, rename column names
        dfs = []
        if "vaccine_data" in data:
            for el in data["vaccine_data"]:
                dfs.append(self._truncate_vaccine_data(el.df))
        df = pd.concat(dfs)

        dfs_age_demographics = []
        if "county_demographics_data" in data:
            for i in range(len(data["county_demographics_data"])):
                dfs_age_demographics.append(self._truncate_demographics_age_data(data["county_demographics_data"][i].df, data["county_names"][i]))
        df_age_demographics = pd.concat(dfs_age_demographics)

        dfs_race_demographics = []
        if "county_demographics_data" in data:
            for i in range(len(data["county_demographics_data"])):
                dfs_race_demographics.append(self._truncate_demographics_race_data(data["county_demographics_data"][i].df, data["county_names"][i]))
        df_race_demographics = pd.concat(dfs_race_demographics)

        dfs_sex_demographics = []
        if "county_demographics_data" in data:
            for i in range(len(data["county_demographics_data"])):
                dfs_sex_demographics.append(self._truncate_demographics_sex_data(data["county_demographics_data"][i].df, data["county_names"][i]))
        df_sex_demographics = pd.concat(dfs_sex_demographics)

        dfs_etn_demographics = []
        if "county_demographics_data" in data:
            for i in range(len(data["county_demographics_data"])):
                dfs_etn_demographics.append(self._truncate_demographics_etn_data(data["county_demographics_data"][i].df, data["county_names"][i]))
        df_etn_demographics = pd.concat(dfs_etn_demographics)

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

        # out = df.melt(id_vars=["location_name"], value_vars=crename.keys()).dropna()
        # out = self.extract_CMU(out, crename)

        out: pd.DataFrame = pd.concat(
            [df_age_demographics, df_etn_demographics, df_race_demographics, df_sex_demographics], axis=0, ignore_index=True
        ).dropna()
        out["vintage"] = self._retrieve_vintage()
        out["dt"] = self._get_date()
        out["category"] = "total_vaccine_initiated"
        out["measurement"] = "new"
        out["unit"] = "people"
        out["value"] = out["total_people_vaccinated_total"]

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

    def _truncate_demographics_age_data(self, data, county_name):

        data.columns = [
            "location_name",
            "age",
            "first_dose_total",
            "series_complete_total",
            "total_people_vaccinated_total",
        ]

        data.loc[:, 'location_name'] = county_name
        startIndex = data.query("age == 'Age group'").index[0] + 1
        result = data[startIndex: startIndex+8]
        result["race"] = result["ethnicity"] = result["sex"] = "all"
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
        result["age"] = result["age"].map(age_replace)

        return result

    def _truncate_demographics_race_data(self, data, county_name):

        data.columns = [
            "location_name",
            "race",
            "first_dose_total",
            "series_complete_total",
            "total_people_vaccinated_total",
        ]

        data.loc[:, 'location_name'] = county_name
        startIndex = data.query("race == 'Race'").index[0] + 1
        result = data[startIndex: startIndex+6]
        result.drop(result[result.race == ""].index, inplace=True)
        result["age"] = result["ethnicity"] = result["sex"] = "all"
        race_replace = {
            "American Indian/Alaskan": "ai_an",
            "Unknown": "unknown",
            "Black": "black",
            "Other": "other",
            "White": "white",
        }
        result["race"] = result["race"].map(race_replace)
        return result

    def _truncate_demographics_sex_data(self, data, county_name):

        data.columns = [
            "location_name",
            "sex",
            "first_dose_total",
            "series_complete_total",
            "total_people_vaccinated_total",
        ]

        data.loc[:, 'location_name'] = county_name
        startIndex = data.query("sex == 'Gender'").index[0] + 1
        result = data[startIndex: startIndex+3]
        result["age"] = result["ethnicity"] = result["race"] = "all"
        gender_replace = {
            "Female": "female",
            "Male": "male",
            "Unknown": "unknown",
        }
        result["sex"] = result["sex"].map(gender_replace)
        return result

    def _truncate_demographics_etn_data(self, data, county_name):

        data.columns = [
            "location_name",
            "ethnicity",
            "first_dose_total",
            "series_complete_total",
            "total_people_vaccinated_total",
        ]

        data.loc[:, 'location_name'] = county_name
        startIndex = data.query("ethnicity == 'Ethnicity'").index[0] + 1
        result = data[startIndex: startIndex+3]
        result["age"] = result["sex"] = result["race"] = "all"
        ethnicity_replace = {
            "Hispanic": "hispanic",
            "Non-Hispanic": "non-hispanic",
            "Unknown": "unknown",
        }
        result["ethnicity"] = result["ethnicity"].map(ethnicity_replace)
        return result