import pandas as pd
import us
from tableauscraper import TableauScraper as TS
from can_tools.scrapers import variables
from can_tools.scrapers.official.base import StateDashboard


class SCVaccineDemographics(StateDashboard):
    has_location = False
    source = "https://scdhec.gov/covid19/covid-19-vaccination-dashboard"
    source_name = "S.C. Department of Health and Environmental Control"
    state_fips = int(us.states.lookup("South Carolina").fips)
    location_type = "county"
    timezone = "US/Eastern"
    fullUrl = f"https://public.tableau.com/views/COVIDVaccineDashboard/RECIPIENTVIEW"
    cmus = {"total_vaccine_initiated": variables.INITIATING_VACCINATIONS_ALL}

    def fetch(self) -> TS:
        ts = TS()
        ts.loads(self.fullUrl)
        return ts

    def normalize(self, data: pd.DataFrame) -> pd.DataFrame:

        # get main sheet
        ws = data.getWorksheet("Vaccine Map By SC residents PEOPLE")
        counties = ws.getFilters()[0]["values"]
        c = pd.DataFrame(counties)

        dfs = []
        for county in counties:
            # set county and get subsheet data
            print("working on: ", county)
            wb = ws.setFilter("Recipient County for maps", county)
            county_view = wb.getWorksheet("Final Age xSex x Race REC").data
            county_view["location_name"] = county
            dfs.append(county_view)
        df = pd.concat(dfs)

        # format data
        cols = {
            "Age Bins SIMON1-alias": "age",
            "AGG(Count individuals with Suppression  for bar graph)-alias": "value",
            "Assigned Race-alias": "race",
            "recip sex (Recipient Data Vaccine Dashboard v1.csv)-alias": "sex",
            "location_name": "location_name",
        }
        df = df[cols.keys()]
        df = (
            df.rename(columns=cols)
            .replace(
                {
                    "65+": "65_plus",
                    "M": "male",
                    "F": "female",
                    "Asian/NHPI/AIAN*": "asian",
                }
            )
            .assign(
                value=lambda x: pd.to_numeric(
                    x["value"].astype(str).str.replace(",", "")
                ),
                race=lambda x: x["race"].str.replace("*", "").str.lower(),
                ethnicity="all",
                category="total_vaccine_initiated",
                measurement="cumulative",
                unit="people",
                vintage=self._retrieve_vintage(),
                dt=self._retrieve_dtm1d(self.timezone),
            )
        )
        # i believe -1 is a placeholder for NA
        return df.query("value != -1 and location_name != 'nan'")
