import re
import tempfile
import pandas as pd
import camelot
import pandas as pd
import requests
import us
import textract

from can_tools.scrapers.official.base import StateDashboard
from can_tools.scrapers import variables, CMU
from typing import Any, Dict


class FloridaCountyVaccine(StateDashboard):
    has_location = False
    source = "https://floridahealthcovid19.gov/#latest-stats"
    location_type = "county"
    state_fips = int(us.states.lookup("Florida").fips)
    fetch_url = "http://ww11.doh.state.fl.us/comm/_partners/covid19_report_archive/vaccine/vaccine_report_latest.pdf"
    source_name = "Florida Department of Health"
    variables = {
        "total_people_vaccinated": variables.INITIATING_VACCINATIONS_ALL,
        "series_complete_total": variables.FULLY_VACCINATED_ALL,
        "total_people_vaccinated_new": CMU(
            category="total_vaccine_initiated",
            measurement="new",
            unit="people",
        ),
        "series_complete_new": CMU(
            category="total_vaccine_completed",
            measurement="new",
            unit="people",
        ),
    }

    def fetch(self) -> camelot.core.TableList:
        return camelot.read_pdf(self.fetch_url, pages="2-end", flavor="stream")

    def normalize(self, data: Any) -> pd.DataFrame:
        # locate where the table starts and parse data
        # loop is needed for if table overflows onto second page
        dfs = []
        for chunk in data:
            df = chunk.df
            header_loc = df.index[df.iloc[:, 0] == "County of residence"].values[0]
            df = df.iloc[header_loc + 2 :, :].reset_index(drop=True)
            dfs.append(df)

        # combine and set colnames
        df = pd.concat(dfs)
        df.columns = [
            "location_name",
            "first_dose_new",
            "series_complete_new",
            "total_people_vaccinated_new",
            "first_dose_total",
            "series_complete_total",
            "total_people_vaccinated",
        ]

        df = self._rename_or_add_date_and_location(
            data=df,
            location_name_column="location_name",
            location_names_to_drop=["Unknown", "Out-Of-State", "Total"],
            location_names_to_replace={"Desoto": "DeSoto", "Dade": "Miami-Dade"},
            date=self._get_date(),
        )
        out = self._reshape_variables(data=df, variable_map=self.variables)
        return out

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


class FloridaCountyVaccineDemographics(FloridaCountyVaccine):

    variables = {
        "series_complete_total": variables.FULLY_VACCINATED_ALL,
        "total_people_vaccinated_total": variables.INITIATING_VACCINATIONS_ALL,
    }
    fetch_url_for_counties = (
        "http://ww11.doh.state.fl.us/comm/_partners/covid19_report_archive/vaccine-county"
        "/vaccine_county_report_latest.pdf"
    )

    def fetch(self) -> Dict[str, camelot.core.TableList]:
        county_names = []
        results = requests.get(self.fetch_url_for_counties)
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_file = "{}/vaccine_county_report_latest.pdf".format(tmp_dir)
            with open(tmp_file, "wb") as f:
                f.write(results.content)
                pdf_pages_headers = textract.process(tmp_file)
                county_names = re.findall(
                    r"COVID-19: (?P<countyName>.*?) vaccine summary",
                    pdf_pages_headers.decode("utf-8"),
                )
                county_names = [x.replace(" County", "") for x in county_names]

        county_demographics_data = camelot.read_pdf(
            self.fetch_url_for_counties, pages="1-end", flavor="stream", row_tol=10
        )
        return {
            "county_demographics_data": county_demographics_data,
            "county_names": county_names,
            "headers": pdf_pages_headers,
        }

    def normalize(self, data):
        dfs = []
        if "county_demographics_data" in data:
            for dataset, name in zip(
                data["county_demographics_data"], data["county_names"]
            ):
                df = dataset.df
                dfs.append(self._truncate_demographics_age_data(df, name))
                dfs.append(self._truncate_demographics_race_data(df, name))
                dfs.append(self._truncate_demographics_sex_data(df, name))
                dfs.append(self._truncate_demographics_etn_data(df, name))
        out = (
            pd.concat(dfs, axis=0, ignore_index=True)
            .dropna()
            .drop(["first_dose_total"], axis="columns")
            .melt(
                id_vars=["location_name", "age", "race", "ethnicity", "sex"],
            )
            .pipe(
                self.extract_CMU,
                skip_columns=["age", "race", "ethnicity", "sex"],
                cmu=self.variables,
            )
            .assign(
                dt=self._get_date(),
                location_type="county",
                vintage=self._retrieve_vintage(),
                value=lambda x: x["value"].str.replace(",", "").astype(int),
            )
            .replace({"location_name": {"Desoto": "DeSoto", "Dade": "Miami-Dade"}})
        )
        out.loc[out["location_name"] == "Florida", "location_type"] = "state"

        return out

    def _truncate_demographics_age_data(self, data, county_name):

        out = data.copy()

        out.columns = [
            "location_name",
            "age",
            "first_dose_total",
            "series_complete_total",
            "total_people_vaccinated_total",
        ]

        out.loc[:, "location_name"] = county_name
        startIndex = out.query("age == 'Age group'").index[0] + 1
        result = out[startIndex : startIndex + 9]
        result["race"] = result["ethnicity"] = result["sex"] = "all"
        age_replace = {
            "12-14 years": "12-14",
            "15-24 years": "15-24",
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

        out = data.copy()

        out.columns = [
            "location_name",
            "race",
            "first_dose_total",
            "series_complete_total",
            "total_people_vaccinated_total",
        ]

        out.loc[:, "location_name"] = county_name
        startIndex = out.query("race == 'Race'").index[0] + 1
        result = out[startIndex : startIndex + 6]
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

        out = data.copy()
        out.columns = [
            "location_name",
            "sex",
            "first_dose_total",
            "series_complete_total",
            "total_people_vaccinated_total",
        ]

        out.loc[:, "location_name"] = county_name
        startIndex = out.query("sex == 'Gender'").index[0] + 1
        result = out[startIndex : startIndex + 3]
        result["age"] = result["ethnicity"] = result["race"] = "all"
        gender_replace = {
            "Female": "female",
            "Male": "male",
            "Unknown": "unknown",
        }
        result["sex"] = result["sex"].map(gender_replace)
        return result

    def _truncate_demographics_etn_data(self, data, county_name):

        out = data.copy()
        out.columns = [
            "location_name",
            "ethnicity",
            "first_dose_total",
            "series_complete_total",
            "total_people_vaccinated_total",
        ]

        out.loc[:, "location_name"] = county_name
        startIndex = out.query("ethnicity == 'Ethnicity'").index[0] + 1
        result = out[startIndex : startIndex + 3]
        result["age"] = result["sex"] = result["race"] = "all"
        ethnicity_replace = {
            "Hispanic": "hispanic",
            "Non-Hispanic": "non-hispanic",
            "Unknown": "unknown",
        }
        result["ethnicity"] = result["ethnicity"].map(ethnicity_replace)
        return result
