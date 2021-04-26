import pandas as pd
from tableauscraper import TableauScraper as TS
from tableauscraper.TableauWorkbook import TableauWorkbook
from us import states

from can_tools.scrapers import variables
from can_tools.scrapers.official.base import CountyDashboard


class PhiladelphaVaccine(CountyDashboard):
    state_fips = int(states.lookup("Pennsylvania").fips)
    has_location = True
    location_type = "county"
    source = (
        "https://www.phila.gov/programs/coronavirus-disease-2019-covid-19/data/vaccine/"
    )
    source_name = "Philadelphia Department of Public Health"

    url = "https://healthviz.phila.gov/t/PublicHealth/views/COVIDVaccineDashboard/COVID_Vaccine?:embed=y&:showVizHome=no&:host_url=https%3A%2F%2Fhealthviz.phila.gov%2F&:embed_code_version=3&:tabs=no&:toolbar=no&:alerts=no&:showShareOptions=false&:showAskData=false&:showAppBanner=false&:isGuestRedirectFromVizportal=y&:display_spinner=no&:loadOrderID=0"
    sheet_name = "Residents Percentage"

    variables = {
        "fully vaccinated residents": variables.FULLY_VACCINATED_ALL,
        "residents recieving at least 1 dose": variables.INITIATING_VACCINATIONS_ALL,
    }

    def fetch(self) -> TableauWorkbook:
        ts = TS()
        ts.loads(self.url)
        return ts.getWorkbook()

    def normalize(self, data: TableauWorkbook) -> pd.DataFrame:
        sheet = data.getWorksheet(self.sheet_name)
        df = sheet.data
        df["Measure Names-alias"] = df["Measure Names-alias"].str.lower().str.strip()
        found = {}
        for target in self.variables.keys():
            bools = df["Measure Names-alias"] == target
            assert bools.sum() == 1
            found[target] = int(
                df.loc[bools, "Measure Values-alias"].iloc[0].replace(",", "")
            )

        return (
            pd.Series(found, name="value")
            .rename_axis("variable")
            .reset_index()
            .pipe(self.extract_CMU, self.variables)
            .assign(location=42101, vintage=self._retrieve_vintage())
            .pipe(
                self._rename_or_add_date_and_location,
                location_column="location",
                timezone="US/Eastern",
            )
            .drop("variable", axis="columns")
        )
