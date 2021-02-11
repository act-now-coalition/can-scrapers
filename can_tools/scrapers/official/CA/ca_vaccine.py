src="https://public.tableau.com/views/COVID-19VaccineDashboardPublic/Vaccine?:embed=y&:showVizHome=no&:host_url=https://public.tableau.com/&:embed_code_version=3&:tabs=no&:toolbar=yes&:animate_transition=yes&:display_static_image=no&:display_spinner=no&:display_overlay=yes&:display_count=yes&:language=en&publish=yes&:loadOrderID=0"


import pandas as pd
import us

from can_tools.scrapers.base import CMU
from can_tools.scrapers.official.base import TableauDashboard


class CaliforniaVaccineCounty(TableauDashboard):
    has_location = False
    source = "https://covidvaccine.oregon.gov/"
    state_fips = int(us.states.lookup("California").fips)
    location_type = "county"

    baseurl = "https://public.tableau.com"

    viewPath = "COVID-19VaccineDashboardPublic/Vaccine"

    cmus = {
        "AGG(Total Doses Administered)-alias": CMU(
            category="total_vaccine_doses_administered",
            measurement="cumulative",
            unit="doses",
        ),
    }

    county_column = "County-value"

    def fetch(self) -> pd.DataFrame:
        return self.get_tableau_view()["County Map"]

    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        # county names (converted to title case)
        df["location_name"] = df[self.county_column].str.title()

        # parse out 1st/2nd dose columns
        value_cols = list(set(df.columns) & set(self.cmus.keys()))
        assert len(value_cols) == 1

        return (
            df.melt(id_vars=["location_name"], value_vars=value_cols)
            .dropna()
            .assign(
                dt=self._retrieve_dt("US/Pacific"),
                vintage=self._retrieve_vintage(),
                value=lambda x: pd.to_numeric(x.loc[:, "value"]),
            )
            .pipe(self.extract_CMU, cmu=self.cmus)
            .drop(["variable"], axis=1)
        )
