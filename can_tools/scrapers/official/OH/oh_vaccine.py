from can_tools.scrapers.base import CMU
import pandas as pd
import us

from can_tools.scrapers.official.base import StateDashboard


class OhioVaccineCounty(StateDashboard):
    has_location = False
    source = "https://coronavirus.ohio.gov/wps/portal/gov/covid-19/dashboards/covid-19-vaccine/covid-19-vaccination-dashboard"
    state_fips = int(us.states.lookup("Ohio").fips)
    url = "https://coronavirus.ohio.gov/static/dashboards/vaccine_data.csv"
    location_type = "county"

    def fetch(self):
        return pd.read_csv(self.url, parse_dates=["date"])

    def normalize(self, data: pd.DataFrame) -> pd.DataFrame:
        cmus = {
            "vaccines_started": CMU(
                category="total_vaccine_initiated",
                measurement="cumulative",
                unit="people",
            ),
            "vaccines_completed": CMU(
                category="total_vaccine_completed",
                measurement="cumulative",
                unit="people",
            ),
        }

        not_counties = ["Out of State", "Unknown"]  # noqa
        dates = list(data["date"].agg([min, max]))
        idx = pd.MultiIndex.from_product(
            [pd.date_range(*dates), sorted(list(data["county"].unique()))],
            names=["dt", "location_name"],
        )

        return (
            data.rename(columns={"county": "location_name", "date": "dt"})
            .set_index(["dt", "location_name"])
            .reindex(idx, fill_value=0)
            .unstack(level=["location_name"])
            .sort_index()
            .cumsum()
            .stack(level=[0, 1])
            .rename("value")  # name the series
            .reset_index()  # convert to long form df
            .rename(columns={"level_1": "variable"})
            .dropna()
            .assign(
                value=lambda x: pd.to_numeric(x.loc[:, "value"]),
                vintage=self._retrieve_vintage(),
                location_name=lambda x: x["location_name"].str.strip(),
            )
            .query("location_name not in @not_counties")
            .pipe(self.extract_CMU, cmu=cmus)
            .drop(["variable"], axis=1)
        )
