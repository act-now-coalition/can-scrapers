import pandas as pd
import us

from can_tools.scrapers.base import CMU
from can_tools.scrapers.official.base import TableauDashboard


class OregonVaccineCounty(TableauDashboard):
    has_location = False
    source = "https://covidvaccine.oregon.gov/"
    state_fips = int(us.states.lookup("Oregon").fips)
    location_type = "county"

    baseurl = "https://public.tableau.com/"
    viewPath = "OregonCOVID-19VaccinationTrends/OregonCountyVaccinationTrends"

    cmus = {
        "sum(metric - in progress)-alias": CMU(
            category="total_vaccine_initiated",
            measurement="cumulative",
            unit="people",
        ),
        "sum(metric - fully vaccinated)-alias": CMU(
            category="total_vaccine_completed",
            measurement="cumulative",
            unit="people",
        ),
    }

    county_column = "Recip Address County-alias"

    def fetch(self) -> pd.DataFrame:
        return pd.concat(self.get_tableau_view())

    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        # county names (converted to title case)
        df["location_name"] = df[self.county_column].str.title()

        # parse out 1st/2nd dose columns
        df.columns = [x.lower() for x in list(df)]
        value_cols = list(set(df.columns) & set(self.cmus.keys()))
        assert len(value_cols) == 2

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
