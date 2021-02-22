import pandas as pd
import us

from can_tools.scrapers.base import CMU
from can_tools.scrapers.official.base import TableauDashboard


class MissouriVaccineCounty(TableauDashboard):
    has_location = False
    source = "https://results.mo.gov/t/COVID19/views/VaccinationsDashboard/Vaccinations"
    state_fips = int(us.states.lookup("Missouri").fips)
    location_type = "county"
    baseurl = "https://results.mo.gov/t/COVID19"
    viewPath = "VaccinationsDashboard/Vaccinations"
    data_tableau_table = "County - Table"

    def normalize(self, data: pd.DataFrame) -> pd.DataFrame:
        _make_cmu = lambda c: CMU(category=c, measurement="cumulative", unit="people")
        cmus = {
            "First COVID-19 Dose Administered": _make_cmu("total_vaccine_initiated"),
            "Second COVID-19 Dose Administered": _make_cmu("total_vaccine_completed"),
        }
        non_counties = ["St. Louis City"]  # noqa
        return (
            data.rename(
                columns={
                    "Measure Values-alias": "value",
                    "Measure Names-alias": "variable",
                    "County Name-value": "location_name",
                }
            )
            .loc[:, ["value", "variable", "location_name"]]
            .pipe(lambda x: x.loc[x.variable.isin(cmus.keys()), :])
            .assign(
                dt=self._retrieve_dt("US/Central"),
                vintage=self._retrieve_vintage(),
                value=lambda x: x["value"].astype(str).str.replace(",", "").astype(int),
                location_name=lambda x: x["location_name"].replace(
                    {"NewMadrid": "New Madrid"}
                ),
            )
            .query("location_name not in @non_counties")
            .pipe(self.extract_CMU, cmu=cmus)
            .drop(["variable"], axis="columns")
        )
