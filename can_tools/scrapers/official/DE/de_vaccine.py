import pandas as pd
import requests
import us
from bs4 import BeautifulSoup
from typing import Dict, List
import re

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
        # the summary/overall table is always the first on the page,
        # since all the tables have the same class name, just access it
        # by index (first in the list)
        return [
            pd.read_html(
                self.url_base.format(county=county),
                attrs={"class": "table c-dash-table__table table-striped"},
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
        "at_least_one_dose": variables.INITIATING_VACCINATIONS_ALL,
        "fully_vaccinated": variables.FULLY_VACCINATED_ALL,
    }

    def fetch(self) -> Dict[str, Dict[str, requests.models.Response]]:
        # each combination of county and dose type has its own page (6 pages total) with the url as below
        url_template = (
            "https://myhealthycommunity.dhss.delaware.gov/locations/"
            "county-{county}/covid19_vaccine_fully_vaccinated/demographics?demographics_stat_type={var}"
        )

        # store responses in dict of dicts like:
        # {'county': {'at_least_one_dose': response, 'fully_vaccinated': response}, ...}
        data = {}
        # for each county, get request for initiated and completed data
        for county in ["kent", "sussex", "new-castle"]:
            urls = {}
            for var in ["at_least_one_dose", "fully_vaccinated"]:
                r = requests.get(url_template.format(county=county, var=var))
                urls[var] = r
            data[county] = urls

        return data

    def _get_demographic(
        self, data: Dict[str, Dict[str, requests.models.Response]], demographic: str
    ) -> pd.DataFrame:
        """
        extract data for each county and dose type for specified demographic
        """
        # loop through each county and each variable for each county and extract data:
        dfs = []
        for county, responses in data.items():
            for var, response in responses.items():
                # find the divs that contain the data tables
                soup = BeautifulSoup(response.text, "lxml")
                divs = soup.find_all("div", class_="c-table-with-chart")

                for div in divs:
                    # find the div that contains the correct demographic data
                    title = div.find("h2", text=re.compile(f"by {demographic}"))
                    if title is not None:
                        # extract table and load into dataframe
                        table = div.find(
                            "table", class_="table c-dash-table__table table-striped"
                        )
                        table = pd.read_html(str(table))[0].assign(
                            variable=var, location_name=county
                        )
                        dfs.append(table)

        return pd.concat(dfs)

    def normalize(
        self, data: Dict[str, Dict[str, requests.models.Response]]
    ) -> pd.DataFrame:
        # for each demographic: get data, format, then append to list
        dfs = []
        for demo in ["sex", "race", "age", "ethnicity"]:

            # get demographic data, create CMU columns
            df = (
                self._get_demographic(data, demo.title())
                .drop(
                    columns={
                        f"% of all persons vaccinated",
                        "% of demographic group vaccinated",
                    }
                )
                .rename(columns={"Count": "value"})
            )
            df.columns = [x.lower() for x in df.columns]
            df = self.extract_CMU(df, cmu=self.variables, skip_columns=[demo])

            # format demographic column and append to list
            df[demo] = df[demo].str.lower().str.replace("*", "")
            dfs.append(df)

        # combine and format total df
        out = pd.concat(dfs)
        out = (
            out.dropna()
            .assign(
                dt=self._retrieve_dtm1d("US/Eastern"),
                vintage=self._retrieve_vintage(),
                value=lambda x: pd.to_numeric(
                    x["value"].astype(str).str.replace(",", "")
                ),
            )
            .drop(columns={"variable"})
        )
        out["location_name"] = out["location_name"].str.title().str.replace("-", " ")
        out = out.replace({"65+": "65_plus", "pacific islander": "pacific_islander"})

        # combine not reported + declined disclosure into 'unknown' values
        group_by = [c for c in out.columns if c != "value"]
        out = out.replace(
            dict.fromkeys(
                ["patient declined to disclose", "data not reported"], "unknown"
            )
        )
        out = out.groupby(group_by, as_index=False).aggregate({"value": "sum"})

        return out
