import pandas as pd
from pandas.core.accessor import register_index_accessor
import us
import requests

from can_tools.scrapers import variables
from can_tools.scrapers.official.base import StateDashboard


class NebraskaCases(StateDashboard):

    has_location = True
    location_type = "county"
    state_fips = int(us.states.lookup("Nebraska").fips)
    source = "https://experience.arcgis.com/experience/ece0db09da4d4ca68252c3967aa1e9dd"
    fetch_url = (
        "https://gis.ne.gov/Enterprise/rest/services/C19Combine/FeatureServer/0/query"
    )
    source_name = "Nebraska Department of Health and Human Services"
    variables = {"county_cases": variables.CUMULATIVE_CASES_PEOPLE}

    def fetch(self) -> requests.models.Response:
        params = {
            "f": "json",
            "where": "0=0",
            "returnGeometry": "false",
            "outFields": "name,Counties_S,PosCases",
        }
        return requests.get(self.fetch_url, params=params)

    def normalize(self, data: requests.models.Response) -> pd.DataFrame:
        # fetch county population table, keep only cols and rows of interest
        county_pops = (
            pd.read_csv(
                "https://media.githubusercontent.com/media/covid-projections/covid-data-public/main/data/misc/fips_population.csv"
            )
            .query("state == 'NE'")
            .assign(county=lambda x: x["county"].str.replace(" County", ""))
            .rename(columns={"population": "county_population"})
            .loc[:, ["county", "fips", "county_population"]]
            .set_index("county")
        )

        return (
            # parse and format dashboard data
            pd.json_normalize(data.json()["features"])
            .assign(county=lambda x: x["attributes.Counties_S"].str.split(", "))
            .drop(columns={"attributes.Counties_S"})
            .rename(
                columns={
                    "attributes.PosCases": "region_cases",
                    "attributes.name": "health_region",
                }
            )
            .explode("county")
            # left join with the county population table on county names
            .set_index("county")
            .join(county_pops, how="left")
            .reset_index()
            # calculate each health region's population (sum pop'n of each county in each region)
            # find % of region's total population for each county
            # multiply % by region's total cases to get estimated number of cases for each county
            .assign(
                region_population=lambda x: (
                    x["health_region"].map(
                        x.groupby("health_region").sum()["county_population"].to_dict()
                    )
                ),
                county_percent_pop=lambda x: (
                    x["county_population"] / x["region_population"]
                ),
                county_cases=lambda x: x["region_cases"] * x["county_percent_pop"],
            )
            # add date, melt into CMU variables
            .pipe(
                self._rename_or_add_date_and_location,
                location_column="fips",
                timezone="US/Central",
            )
            .pipe(self._reshape_variables, variable_map=self.variables)
        )
