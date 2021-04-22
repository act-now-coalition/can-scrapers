import asyncio
import pandas as pd


from can_tools.scrapers.puppet import with_page
from can_tools.scrapers.base import CMU, RequestError
from can_tools.scrapers.official.base import FederalDashboard
from can_tools.scrapers import variables


class CDCUSAVaccine(FederalDashboard):
    has_location = True
    location_type = "nation"
    source = r"https://covid.cdc.gov/covid-data-tracker/#vaccinations"
    source_name = "Centers for Disease Control and Prevention"
    provider = "cdc"

    variables = {
        "fully_vaccinated": variables.FULLY_VACCINATED_ALL,
        "at_least_one": variables.INITIATING_VACCINATIONS_ALL,
    }

    async def fetch_with_puppet(self, headless=True):
        async with with_page(headless=headless) as page:
            await page.goto(self.source)
            fullyvac = await page.waitForXPath(
                r'//*[@id="vaccinations-banner-wrapper"]/div[1]/div/div[2]/div/div/div[2]/div[3]/div'
            )

            atleastone = await page.waitForXPath(
                r'//*[@id="vaccinations-banner-wrapper"]/div[1]/div/div[2]/div/div/div[2]/div[2]/div'
            )
            func = "(x) => x.textContent"
            out = {
                "fully_vaccinated": await page.evaluate(func, fullyvac),
                "at_least_one": await page.evaluate(func, atleastone),
            }
        return out

    def fetch(self, headless=True) -> dict:
        return asyncio.run(self.fetch_with_puppet(headless=headless))

    def normalize(self, data: dict):

        for k, v in data.items():
            data[k] = int(v.replace(",", ""))

        return (
            pd.Series(data, name="value")
            .rename_axis("variable")
            .reset_index()
            .pipe(self.extract_CMU, self.variables)
            .assign(location=0, vintage=self._retrieve_vintage())
            .pipe(
                self._rename_or_add_date_and_location,
                location_column="location",
                timezone="US/Eastern",
            )
            .drop("variable", axis="columns")
        )


def one_time_backfill():
    df = pd.read_csv(
        "/home/sglyon/Downloads/trends_in_number_of_covid19_vaccinations_in_the_us.csv",
        skiprows=2,
    )
    filtered = df.loc[(df["Date Type"] == "Admin") & (df["Program"] == "US")]
    variable_map = {
        "People with at least One Dose Cumulative": variables.INITIATING_VACCINATIONS_ALL,
        "People Fully Vaccinated Cumulative": variables.FULLY_VACCINATED_ALL,
    }

    d = CDCUSAVaccine()
    cols = list(variable_map.keys()) + ["Date"]
    df = (
        filtered.loc[:, cols]
        .assign(location=0)
        .pipe(
            d._rename_or_add_date_and_location,
            location_column="location",
            date_column="Date",
        )
        .pipe(d._reshape_variables, variable_map)
    )
    return df
