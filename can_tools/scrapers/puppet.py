import asyncio
import glob
import io
import os
import tempfile
from abc import ABC
from contextlib import asynccontextmanager

import pandas as pd

import pyppeteer

from .base import DatasetBase

CHROME_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_2)"
    + " AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36"
)
DEFAULT_VIEWPORT = {"width": 1280, "height": 800, "isMobile": False}


def xpath_class_check(cls: str) -> str:
    return f"contains(concat(' ',normalize-space(@class),' '),' {cls} ')"


@asynccontextmanager
async def with_page(headless: bool = True) -> pyppeteer.page.Page:
    browser = await pyppeteer.launch(headless=headless)
    try:
        page = await browser.newPage()
        await page.setUserAgent(CHROME_AGENT)
        await page.setViewport(DEFAULT_VIEWPORT)
        yield page
    finally:
        await browser.close()


class TableauNeedsClick(DatasetBase, ABC):
    url: str
    headless: bool = True
    download_button_id: str = "#download-ToolbarButton"
    crosstab_button_selector: str = "@data-tb-test-id = 'DownloadCrosstab-Button'"

    async def _pre_click(self, page: pyppeteer.page.Page):
        pass

    async def click_table(
        self,
        page: pyppeteer.page.Page,
        x: int = 281,
        y: int = 350,
        repeats: int = 3,
        delay: int = 2,
    ):
        for i in range(repeats):
            await asyncio.sleep(delay)
            await page.mouse.click(x, y, options={"delay": 100})

    async def _post_download(self, tmpdirname: str) -> pd.DataFrame:
        fns = glob.glob(os.path.join(tmpdirname, "*.csv"))
        assert len(fns) == 1
        with open(fns[0], "rb") as f:
            tsv = io.BytesIO(f.read().decode("UTF-16").encode("UTF-8"))
            df = pd.read_csv(tsv, sep="\t")

        return df

    def _clean_df(self, df: pd.DataFrame) -> pd.DataFrame:
        return df

    async def _get(self) -> pd.DataFrame:
        with tempfile.TemporaryDirectory() as tmpdirname:
            async with with_page(headless=self.headless) as page:
                await page.goto(self.url)
                await page._client.send(
                    "Page.setDownloadBehavior",
                    {"behavior": "allow", "downloadPath": tmpdirname},
                )

                await self._pre_click(page)

                await self.click_table(page)

                # click download button to open dialog
                button = await page.waitForSelector(self.download_button_id)
                await button.click()

                # find and click crosstab option to initiate download
                crosstab = await page.waitForXPath(
                    f"//button[{self.crosstab_button_selector}]"
                )
                await crosstab.click()

                # TODO: better way to wait for download?
                await asyncio.sleep(5)

                # call the post_download method to process download
                df = await self._post_download(tmpdirname)

        return df

    def get(self) -> pd.DataFrame:
        return self._clean_df(asyncio.run(self._get()))
