from typing import Tuple
import pandas as pd
import us
from tableauscraper import TableauScraper
from tableauscraper.TableauWorkbook import TableauWorkbook
from can_tools.scrapers.base import CMU
from can_tools.scrapers import variables
from can_tools.scrapers.official.base import StateDashboard


class OHVaccineCountyRace(StateDashboard):
    has_location = False
    source = "https://coronavirus.ohio.gov/wps/portal/gov/covid-19/dashboards/covid-19-vaccine/covid-19-vaccination-dashboard"
    source_name = "Ohio State Department of Health"
    state_fips = int(us.states.lookup("Ohio").fips)
    location_type = "county"
    timezone = "US/Eastern"
    url = r"https://public.tableau.com/views/VaccineAdministrationMetricsDashboard/PublicCountyDash?:embed=y&:showVizHome=no&:host_url=https%3A%2F%2Fpublic.tableau.com%2F&:embed_code_version=3&:device=desktop&:tabs=no&:toolbar=no&:showAppBanner=false&iframeSizedToWindow=true&:loadOrderID=0"
    demographic_col_name = "Race"
    demographic = "race"
    rename_key = {
        "black or african american": "black",
        "american indian alaska native": "ai_an",
        "native hawaiian pacific islander": "pacific_islander",
        "multiracial": "multiple",
    }

    # map wide form column names into CMUs
    variables = {
        "initiated": variables.INITIATING_VACCINATIONS_ALL,
        "complete": variables.FULLY_VACCINATED_ALL,
    }

    def _extract_data(self, shot_type: str) -> pd.DataFrame:
        ts = TableauScraper()
        ts.loads(self.url)

        # set type of metric and demographic (initiated is fetched by default)
        book = ts.getWorkbook()
        # get all possible paramaters
        params = pd.DataFrame(book.getParameters())
        shot_vals = params.loc[params["column"] == "View By", "values"].iloc[0]
        demo_vals = params.loc[params["column"] == "Key Metrics", "values"].iloc[0]
        assert self.demographic_col_name in demo_vals
        book.setParameter("Key Metrics", self.demographic_col_name)

        if shot_type == "complete":
            print(f"set parameter to: {shot_vals[1]}")
            book.setParameter("View By", shot_vals[1])

        parts = []
        ws = ts.getWorksheet("New Map")
        counties = ws.getSelectableValues("county")
        for county in counties:
            print("working on county: ", county)
            wb = ws.select("county", county)
            df = wb.getWorksheet("New Demographics").data.assign(location_name=county)
            df = df.rename(columns={"SUM(Chosen Dose Status Metric)-alias": shot_type})
            assert county in wb.getWorksheet("New County or Statewide").data.iloc[0, 0]
            parts.append(df)
            ws = ts.getWorksheet("New Map")
        return pd.concat(parts)

    def fetch(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        init = self._extract_data("initiated")
        complete = self._extract_data("complete")
        return init, complete

    def normalize(self, data: Tuple[pd.DataFrame, pd.DataFrame]) -> pd.DataFrame:
        df = pd.merge(
            data[0],
            data[1],
            how="left",
            on=["Selected Demographic-alias", "location_name"],
        )
        df = df[
            ["location_name", "Selected Demographic-alias", "initiated", "complete"]
        ]
        df["Selected Demographic-alias"] = df["Selected Demographic-alias"].str.lower()
        if self.rename_key:
            df = df.replace(self.rename_key)

        out = (
            df.melt(
                id_vars=["location_name", "Selected Demographic-alias"],
                value_vars=self.variables.keys(),
            )
            .dropna()
            .assign(
                value=lambda x: pd.to_numeric(
                    x["value"].astype(str).str.replace(",", "")
                ),
            )
            .pipe(self.extract_CMU, cmu=self.variables)
            .drop(["variable"], axis=1)
        )
        out["dt"] = self._retrieve_dt(self.timezone)
        out["vintage"] = self._retrieve_vintage()

        return out.drop(columns={self.demographic}).rename(
            columns={"Selected Demographic-alias": self.demographic}
        )


class OHVaccineCountySex(OHVaccineCountyRace):
    demographic_col_name = "Sex"
    demographic = "sex"
    rename_key = {}


class OHVaccineCountyAge(OHVaccineCountyRace):
    demographic_col_name = "Age Group"
    demographic = "age"
    rename_key = {"80+": "80_plus"}


class OHVaccineCountyEthnicity(OHVaccineCountyRace):
    demographic_col_name = "Ethnicity"
    demographic = "ethnicity"
    rename_key = {
        "not hispanic or latino": "non-hispanic",
        "hispanic or latino": "hispanic",
    }
