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
    viewPath = "VaccinesAdministeredtoWIResidents/VaccinatedWisconsin-County"

    data_tableau_table = "Map"
    location_name_col = "County-alias"
    timezone = "US/Central"

    # map wide form column names into ScraperVariables
    cmus = {
        "SUM(Number With One Dose)-alias": variables.INITIATING_VACCINATIONS_ALL,
        "SUM(Number With Two Doses)-alias": variables.FULLY_VACCINATED_ALL,
    }

    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        # county names (converted to title case)
        df["location_name"] = df[self.location_name_col].str.title()
        # remove "County" from location_name if it is included
        df["location_name"] = df["location_name"].apply(
            lambda s: s[:-7] if s.endswith("County") else s
        )
        df = df.replace(
            {"location_name": {"St Croix": "St. Croix", "Fond Du Lac": "Fond du Lac"}}
        )  # fix incorrect formatting

        # parse out data columns
        value_cols = list(set(df.columns) & set(self.cmus.keys()))
        assert len(value_cols) == len(self.cmus)

        df = (
            df.melt(id_vars=["location_name"], value_vars=value_cols)
            .dropna()
            .assign(
                dt=self._retrieve_dt(self.timezone),
                vintage=self._retrieve_vintage(),
                value=lambda x: pd.to_numeric(
                    x["value"].astype(str).str.replace(",", "")
                ),
            )
            .pipe(self.extract_scraper_variables, cmu=self.cmus)
            .drop(["variable"], axis=1)
        )
        return df
