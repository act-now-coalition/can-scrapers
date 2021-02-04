import asyncio
from can_tools.scrapers.base import CMU

import us
import pandas as pd

from can_tools.scrapers.official.base import StateDashboard
from can_tools.scrapers.puppet import with_page


class WashingtonVaccine(StateDashboard):
    has_location = False
    location_type = "county"
    state_fips = int(us.states.lookup("Washington").fips)
    source = "https://www.doh.wa.gov/Emergencies/COVID19/DataDashboard"

    async def _get_from_browser(self):
        async with with_page(headless=True) as page:
            await page.goto(
                "https://www.doh.wa.gov/Emergencies/COVID19/DataDashboard#downloads"
            )
            sel = "#accVaccinationsTbl table"
            table_div = await page.waitForSelector(sel)
            print("found table!")
            table = await page.J(sel)
            return await page.evaluate(" x => x.outerHTML", table)

    def fetch(self):
        return asyncio.get_event_loop().run_until_complete(self._get_from_browser())

    def normalize(self, data: str) -> pd.DataFrame:
        _cmu = lambda c: CMU(category=c, measurement="cumulative", unit="pepole")
        cmus = {
            "People Initiating Vaccination": _cmu("total_vaccine_initiated"),
            "People Fully Vaccinated": _cmu("total_vaccine_completed"),
        }
        df = (
            pd.read_html(data)[0]
            .query("County != 'Total'")
            .rename(columns={"County": "location_name"})
            .melt(id_vars=["location_name"], value_vars=cmus.keys())
            .assign(
                dt=self._retrieve_dt("America/Los_Angeles"),
                vintage=self._retrieve_vintage(),
            )
            .pipe(self.extract_CMU, cmu=cmus)
            .drop(["variable"], axis="columns")
        )