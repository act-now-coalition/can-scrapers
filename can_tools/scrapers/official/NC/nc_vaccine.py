import pandas as pd
import us

from can_tools.scrapers.base import CMU
from can_tools.scrapers.official.base import TableauDashboard


class NCVaccine(TableauDashboard):
    has_location = False
    source = "https://covid19.ncdhhs.gov/dashboard/vaccinations"
    source_name = (
        "North Carolina Department of Health and Human Services Covid-19 Response"
    )
    state_fips = int(us.states.lookup("North Carolina").fips)
    location_type = "county"
    baseurl = "https://public.tableau.com"
    viewPath = "NCDHHS_COVID-19_Dashboard_Vaccinations/Summary"

    data_tableau_table = "County Map"
    location_name_col = "County -alias"
    timezone = "US/Eastern"

    # map wide form column names into CMUs
    cmus = {
        "SUM(Partially Vaccinated)-alias": CMU(
            category="total_vaccine_initiated",
            measurement="cumulative",
            unit="people",
        ),
        "SUM(Fully Vaccinated)-alias": CMU(
            category="total_vaccine_completed",
            measurement="cumulative",
            unit="people",
        ),
    }

    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        df = super().normalize(df)
        df.location_name = df.location_name.str.replace(
            " County", ""
        ).str.strip()  # Strip whitespace
        df.loc[df["location_name"] == "Mcdowell", "location_name"] = "McDowell"
        return df
