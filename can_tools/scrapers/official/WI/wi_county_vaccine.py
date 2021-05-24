import pandas as pd
import us

from can_tools.scrapers import variables
from can_tools.scrapers.official.base import TableauDashboard


class WisconsinVaccineCounty(TableauDashboard):
    has_location = False
    source = "https://www.dhs.wisconsin.gov/covid-19/vaccine-data.htm#summary"
    source_name = "Wisconsin Department of Health Services"
    state_fips = int(us.states.lookup("Wisconsin").fips)
    location_type = "county"
    baseurl = "https://bi.wisconsin.gov/t/DHS"
    viewPath = (
        "VaccinesAdministeredtoWIResidents_16129838459350/VaccinatedWisconsin-County"
    )

    data_tableau_table = "**Download Table"
    location_name_col = "County-alias"
    timezone = "US/Central"

    # map wide form column names into CMUs
    cmus = {
        "At Least One Dose (#)": variables.INITIATING_VACCINATIONS_ALL,
        "Completed Series (#)": variables.FULLY_VACCINATED_ALL,
    }

    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        # county names (converted to title case)
        df["location_name"] = df[self.location_name_col].str.title()
        # remove "County" from location_name if it is included
        df["location_name"] = df["location_name"].apply(
            lambda s: s[:-7] if s.endswith("County") else s
        )
        df = df[["Measure Values-alias", "location_name", "Measure Names-alias"]]

        df = (
            df.replace(
                {
                    "location_name": {
                        "St Croix": "St. Croix",
                        "Fond Du Lac": "Fond du Lac",
                    }
                }
            )
            .query(
                "`Measure Names-alias` in @self.cmus.keys() and location_name != 'Unknown'"
            )
            .rename(
                columns={
                    "Measure Values-alias": "value",
                    "Measure Names-alias": "variable",
                }
            )
        )

        # parse out data columns
        df = (
            df.dropna()
            .assign(
                dt=self._retrieve_dt(self.timezone),
                vintage=self._retrieve_vintage(),
                value=lambda x: pd.to_numeric(
                    x["value"].astype(str).str.replace(",", "")
                ),
            )
            .pipe(self.extract_CMU, cmu=self.cmus)
            .drop(["variable"], axis=1)
        )
        return df
