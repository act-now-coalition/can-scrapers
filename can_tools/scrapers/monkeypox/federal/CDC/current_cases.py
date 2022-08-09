from can_tools.scrapers.base import CMU
from can_tools.scrapers.official.base import FederalDashboard
import pandas as pd
from can_tools.scrapers.puppet import with_page
import asyncio
import us


def _lookup(location):
    if location == "US":
        return 0
    return int(us.states.lookup(location).fips)


class CDCCurrentMonkeypoxCases(FederalDashboard):
    data_type = "monkeypox"
    has_location = True
    location_type = "state"
    source = "https://www.cdc.gov/poxvirus/monkeypox/response/2022/us-map.html"
    fetch_url = "https://www.cdc.gov/poxvirus/monkeypox/response/modules/MX-response-case-count-US.json"
    source_name = "Centers for Disease Control and Prevention"
    provider = "cdc"

    variables = {
        "CasesSort by cases in no order": CMU(
            category="monkeypox_cases",
            measurement="cumulative",
            unit="people",
        )
    }

    async def _get_table_from_browser(self):
        async with with_page(headless=True) as page:
            await page.goto(self.source)
            table = await page.J(".data-table")
            return await page.evaluate("x => x.outerHTML", table)

    def fetch(self):
        return asyncio.get_event_loop().run_until_complete(
            self._get_table_from_browser()
        )

    def normalize(self, data: pd.DataFrame) -> pd.DataFrame:
        data = pd.read_html(data)[0].assign(
            location=lambda row: row["LocationSort by location in no order"].map(
                _lookup
            ),
            dt=self._retrieve_dt(),
        )
        return self._reshape_variables(data=data, variable_map=self.variables)
