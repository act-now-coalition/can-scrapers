"""
Utilities for working with headless browsers via the pyppeteer library
"""
import asyncio
import glob
import io
import os
import logging
import tempfile
from abc import ABC
from typing import List

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
        args = [
                "--disable-dev-shm-usage",
                "--disable-setuid-sandbox",
                "--disable-gpu",
                "--no-zygote",
            ]

        if os.name != 'nt':
            # Windows does not work with these arguments
            args.append("--single-process")
            args.append("--no-sandbox")
                
        self.browser = await pyppeteer.launch(
            headless=self.headless,
            args=args,
            # Enable for debugging
            # logLevel=logging.DEBUG
        )
        page = await self.browser.newPage()
        await page.setUserAgent(CHROME_AGENT)
        await page.setViewport(DEFAULT_VIEWPORT)

        # Required if site is using F5 Big-IP ASM. 
        # You can tell by looking at the cookies and if you see one called
        # TSxxxxxxxxxx and/or BIGipServerSDC_xxxxxxx, 
        # https://stackoverflow.com/questions/52330611/anyone-know-what-a-ts-cookie-is-and-what-kind-of-data-its-for
        await page.setBypassCSP(True)

        return page

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.browser.close()


class TableauNeedsClick(DatasetBase, ABC):
    """
    Extract data from a tableau dashboard when the download button only
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

    tableau_click_selector_list: List[str]
        The css selector(s) to be used in order to find the button to download
        the data and choose the desired formatting options.
    """

    url: str
    headless: bool = True
    download_button_id: str = "#download-ToolbarButton"
    tableau_click_selector_list: List[str] = [
    	# Default is just clicking the Crosstab button
        "@data-tb-test-id = 'DownloadCrosstab-Button'"
    ]

    async def _pre_click(self, page: pyppeteer.page.Page):
        pass

    async def click_table(
        self,
        page: pyppeteer.page.Page,
        x: int = 281,
        y: int = 350,
        repeats: int = 2,
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
            The number of times to repeat the click action.
            Should keep this an even int in case data is selected,
            clicking it an additional time wil unselect it.
            Selected cells may affect what data is downloaded.
        delay : int
            The delay between clicks
        """
        for _ in range(repeats):
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
            try:
                # Downloaded data from Crosstab needs this
                tsv = io.BytesIO(f.read().decode("UTF-16").encode("UTF-8"))
            except Exception as e:
                # Downloaded data from Data doesn't
                f.seek(0)
                tsv = io.BytesIO(f.read())
            df = pd.read_csv(tsv, sep="\t")

        # TODO0: ZW: Should we be deleting the tempfile once we are done loading it?
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

                for click_selector in self.tableau_click_selector_list:
                    if hasattr(click_selector, '__call__'):
                        # Use for special handling in an instance specific function
                        await click_selector(page, tmpdirname)
                    else:
                        selector = await page.waitForXPath(
                            f"//*[{click_selector}]"
                        )
                        await selector.click()
                        await asyncio.sleep(2)

                # Wait for download to finish. 10 cycles of 2 seconds is arbitrary
                for _ in range(10):
                    if len(os.listdir(tmpdirname)) > 0:
                        break
                    await asyncio.sleep(2)
                else:
                    # TODO0: ZW: Throw / report error?
                    pass



                # call the post_download method to process download
                df = await self._post_download(tmpdirname)

        return df

    def fetch(self) -> pd.DataFrame:
        """
        Open tableau dashboard at url, fetch click around at a specified a few times,
        find and click download button, read downloaded csv into pandas DataFrame
        and return

        Returns
        -------
        df: pandas DataFrame
            The DataFrame in the crosstab table on the tableau dashboard

        """
        return asyncio.get_event_loop().run_until_complete((self._get()))
