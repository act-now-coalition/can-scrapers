"""
Utilities for working with headless browsers via the pyppeteer library
"""
import asyncio
import glob
import io
import os
import tempfile
from abc import ABC

import pandas as pd

import pyppeteer

from can_tools.scrapers.base import DatasetBase

CHROME_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_2)"
    + " AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36"
)
DEFAULT_VIEWPORT = {"width": 1280, "height": 800, "isMobile": False}


def xpath_class_check(cls: str) -> str:
    """

    Parameters
    ----------
    cls : str
        The CSS class name to check

    Returns
    -------
    xpath: str
        An xpath expression for finding an element with class `cls`

    """
    return f"contains(concat(' ',normalize-space(@class),' '),' {cls} ')"


class with_page:
    def __init__(self, headless: bool = True):
        self.headless = headless

    async def __aenter__(self):
        self.browser = await pyppeteer.launch(headless=self.headless)
        page = await self.browser.newPage()
        await page.setUserAgent(CHROME_AGENT)
        await page.setViewport(DEFAULT_VIEWPORT)
        return page

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.browser.close()


class TableauNeedsClick(DatasetBase, ABC):
    """
    Extract data from a tablequ dashboard when the download button only
    appears after some user/mouse interactions with the dashboard

    Attributes
    ----------
    url: str
        The url for the tableau dashboard. Must be set in subclass

    headless: bool
        Whether the page should be opened via a headless browser or a
        standard browser (headless by default, convenient to set to
        false for debugging). Must be set in subclass

    download_button_id: str
        The html #id for the download button. Has a default value that
        seems consistent for the dashbaords we've worked with

    crosstab_button_selector: str
        The css selector to be used in order to find the button to download
        the crosstab table. Again, has a default value that seems consistent,
        but may need to be overriden by a subclass
    """

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
        """
        Click the page at a specified location, a certain number
        of times, with delay betweeen each click

        Parameters
        ----------
        page : pyppeteer.page.Page
            The pyppeteer page to click
        x : int
            The x coordinate for where to click on the page
        y : int
            The y coordinate for where to click on the page
        repeats : int
            The number of times to repeat the click action
        delay : int
            The delay between clicks
        """
        for i in range(repeats):
            await asyncio.sleep(delay)
            await page.mouse.click(x, y, options={"delay": 100})

    async def _post_download(self, tmpdirname: str) -> pd.DataFrame:
        """
        After the csv file has been downloaded, read it from the temp
        directory and return a pandas DataFrame

        Parameters
        ----------
        tmpdirname : str
            The directory where the csv file should be found

        Returns
        -------
        df: pandas.DataFrame
            The pandas DataFrame containing the data from the first csv file
            in `tmpdirname`

        """
        fns = glob.glob(os.path.join(tmpdirname, "*.csv"))
        assert len(fns) == 1
        with open(fns[0], "rb") as f:
            tsv = io.BytesIO(f.read().decode("UTF-16").encode("UTF-8"))
            df = pd.read_csv(tsv, sep="\t")

        return df

    def _clean_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """No cleaning needed by default -- here for subclasses to override"""
        return df

    async def _get(self) -> pd.DataFrame:
        """
        Async function to actually fetch the data from tableau

        Returns
        -------
        df: pandas.DataFrame

        """
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
        """
        Open tableau dashboard at url, fetch click around at a specified a few times,
        find and click download button, read downloaded csv into pandas DataFrame
        and return

        Returns
        -------
        df: pandas DataFrame
            The DataFrame in the crosstab table on the tableau dashboard

        """
        return self._clean_df(asyncio.get_event_loop().run_until_complete((self._get())))
