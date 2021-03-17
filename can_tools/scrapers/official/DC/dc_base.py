import pandas as pd
import us
import lxml.html
import requests
from abc import abstractmethod
from can_tools.scrapers.official.base import StateDashboard


class DCBase(StateDashboard):
    has_location = False
    state_fips = us.states.lookup("DC").fips
    location_type = "state"
    source = "https://coronavirus.dc.gov/page/coronavirus-data"
    source_name = "Government of the District of Columbia"

    def _make_dt_date_and_drop(self, df):
        df["dt"] = pd.to_datetime(df["dt"], errors="coerce")
        return df.dropna(subset=["dt"])

    def fetch(self) -> requests.models.Response:
        """
        locate most recent data export, download excel file

        Returns
        -------
        xl: requests.models.Response
            An html response object containing the downloaded data

        Notes
        -----
        WINDOWS USES %# TO REMOVE LEADING 0'S, UNIX/LINUX USES %-
        strftime() (and other date functions) use C implementations which are sys dependent
        see https://docs.microsoft.com/en-us/cpp/c-runtime-library/reference/strftime-wcsftime-strftime-l-wcsftime-l?view=msvc-160 for info
        """

        # get yesterday's date for finding most recent file
        date = self._retrieve_dtm1d("US/Eastern")

        # query DC coronavirus webpage
        res = requests.get(self.source)
        if not res.ok:
            raise ValueError("Could not fetch source of DC page")

        # find COVID data download links for specified date
        tree = lxml.html.fromstring(res.content)
        xp = "//a[contains(@href,'{date:%B-%-d-%Y}')]"
        links = tree.xpath(xp.format(date=date))

        # if no matching links are returned, take the most recently posted link
        # if multiple are returned, take the most recent of the returned
        # although, almost ALWAYS one link is returned
        if len(links) == 0:
            # raise ValueError(f"no links returned for {date} date")
            xl_src = tree.xpath('//a[contains(text(), "Download copy of")]/@href')[0]
        else:
            xl_src = links[0].attrib["href"]

        # add domain name to download link if necessary
        if xl_src.startswith("https"):
            pass
        elif xl_src.startswith("/"):
            xl_src = "https://coronavirus.dc.gov" + xl_src
        else:
            raise ValueError("Could not parse download link")

        # download file from selected link
        xl = requests.get(xl_src)
        if not xl.ok:
            raise ValueError("Could not fetch download file")

        return xl  # return request response

    def _reshape(self, data: pd.DataFrame, _map: dict) -> pd.DataFrame:
        """
        Function to prep data for put() function. renames and adds columns according to CMU (map) entries

            Accepts
            -------
                data: df w/ column names according the _map parameter
                    example of format:
                                dt   Variable Name  ...         location_name
                    0   2020-03-07  Variable Value  ...  District of Columbia
                    ...
                _map: dictionary with CMU keys/values

            Returns
            -------
                pd.Dataframe: dataframe ready for put() function
        """

        out = data.melt(
            id_vars=["dt", "location_name"], value_vars=_map.keys()
        ).dropna()
        out["value"] = pd.to_numeric(
            out["value"].astype(str).str.replace(",", "").str.strip(),
        )

        out = self.extract_CMU(out, _map)
        out["vintage"] = self._retrieve_vintage()

        cols_to_keep = [
            "vintage",
            "dt",
            "location_name",
            "category",
            "measurement",
            "unit",
            "age",
            "race",
            "ethnicity",
            "sex",
            "value",
        ]

        return out.loc[:, cols_to_keep]

    @abstractmethod
    def _wrangle(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Parent function to re-structure df into standard form.
        Used in child class to transpose and clean data returned from DC's excel data exports
            Accepts
            -------
                data: pd.Dataframe
            Returns
            -------
                pd.DataFrame
        """
