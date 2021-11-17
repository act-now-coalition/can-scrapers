import pandas as pd
import requests
import us
from bs4 import BeautifulSoup
from typing import Dict, List
import re
from itertools import product

from can_tools.scrapers.official.base import StateDashboard
from can_tools.scrapers import variables


class DelawareCountyVaccine(StateDashboard):
    url_base = "https://myhealthycommunity.dhss.delaware.gov/locations/county-{county}/covid19_vaccine_fully_vaccinated"
    has_location = False
    location_type = "county"
    source = "https://myhealthycommunity.dhss.delaware.gov"
    source_name = "Delaware Health and Social Services"
    state_fips = int(us.states.lookup("Delaware").fips)

    variables = {
        "At Least One Dose": variables.INITIATING_VACCINATIONS_ALL,
        "Fully Vaccinated": variables.FULLY_VACCINATED_ALL,
    }

    def fetch(self) -> List[pd.DataFrame]:
        return [
            pd.read_html(
                self.url_base.format(county=county),
                attrs={"class": "table c-dash-table__table c-dash-table--striped-by-2"},
            )[0].assign(location_name=county)
            for county in ("kent", "new-castle", "sussex")
        ]

    def normalize(self, data: List[pd.DataFrame]) -> pd.DataFrame:
        return (
            pd.concat(data)
            .query("`People Vaccinated` == 'All ages'")
            .assign(
                location_name=(
                    lambda row: row["location_name"].str.title().str.replace("-", " ")
                ),
            )
            .pipe(
                self._rename_or_add_date_and_location,
                timezone="US/Eastern",
                location_name_column="location_name",
            )
            .pipe(self._reshape_variables, variable_map=self.variables)
        )


class DelawareVaccineDemographics(DelawareCountyVaccine):

    variables = {
        "have_received_at_least_one_dose": variables.INITIATING_VACCINATIONS_ALL,
        "are_fully_vaccinated": variables.FULLY_VACCINATED_ALL,
    }

    def fetch(self) -> Dict[str, requests.models.Response]:
        # each combination of county and dose type has its own page (6 pages total) with the url as below
        url_template = "https://myhealthycommunity.dhss.delaware.gov/locations/county-{county}/covid19_vaccine_demographics"

        # store responses in dict of dicts:
        data = {}
        for county in ["kent", "sussex", "new-castle"]:
            res = requests.get(url_template.format(county=county))
            data[county] = res
        return data

    def _get_demographic(
        self, data: Dict[str, requests.models.Response]
    ) -> pd.DataFrame:
        """
        extract data for each county and dose type for all demographics
        """

        dfs = []
        for county, response in data.items():
            # get all combinations of demographics and variables
            for var, dose in list(
                product(
                    ["Race", "Sex", "Age", "Ethnicity"],
                    ["Have received at least one dose", "Are fully vaccinated"],
                )
            ):
                # find the divs that contain the data tables
                soup = BeautifulSoup(response.text, "lxml")
                divs = soup.find_all("div", class_="c-table-with-chart__wrapper")

                for div in divs:
                    # find the div that contains the correct demographic data
                    title = div.find(
                        "h3", text=re.compile(f"% of Delawareans by {var} who {dose}")
                    )
                    if title is not None:
                        table = div.find(
                            "table",
                            class_="table c-dash-table__table c-dash-table--striped",
                        )
                        table = pd.read_html(str(table))[0].assign(
                            variable=dose, location_name=county
                        )
                        dfs.append(table)
        return pd.concat(dfs)

    def normalize(self, data: Dict[str, requests.models.Response]) -> pd.DataFrame:
        demographic_cols = ["race", "sex", "age", "ethnicity"]
        # fetch demographic tables and format dataframe variables
        df = (
            self._get_demographic(data)
            .rename(columns={"Count": "value"})
            .drop(
                columns={
                    "% of demographic group vaccinated",
                    "% of all persons vaccinated",
                }
            )
        )

        df.columns = [col.lower() for col in df.columns]
        df[demographic_cols] = df[demographic_cols].fillna(value="all")
        df = df.applymap(
            lambda string: string.lower().replace(" ", "_").replace("*", "")
            if type(string) == str
            else string
        )

        # expand into CMU variables and format
        return (
            self.extract_CMU(
                df=df,
                cmu=self.variables,
                skip_columns=demographic_cols,
            )
            .drop(columns={"variable"})
            .pipe(
                self._rename_or_add_date_and_location,
                location_name_column="location_name",
                timezone="US/Eastern",
            )
            .assign(
                vintage=self._retrieve_vintage().tz_localize(None),
                location_name=lambda row: row["location_name"].str.replace("-", " "),
                age=lambda row: row["age"].str.replace("+", "_plus"),
                dt=lambda row: row["dt"].dt.date,
            )
            .query(
                'race not in ["patient_declined_to_disclose", "data_not_reported"] and '
                'ethnicity not in ["patient_declined_to_disclose", "data_not_reported"]'
            )
            .dropna()
        )
