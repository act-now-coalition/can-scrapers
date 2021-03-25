import asyncio

import pandas as pd

from can_tools.scrapers.puppet import with_page


async def main():
    async with with_page(headless=True) as page:
        await page.goto(
            "https://www.doh.wa.gov/Emergencies/COVID19/DataDashboard#downloads"
        )
        sel = "#accVaccinationsTbl table"
        table_div = await page.waitForSelector(sel)
        print("found table!")
        table = await page.J(sel)
        return await page.evaluate(" x => x.outerHTML", table)


table_text = asyncio.get_event_loop().run_until_complete(main())