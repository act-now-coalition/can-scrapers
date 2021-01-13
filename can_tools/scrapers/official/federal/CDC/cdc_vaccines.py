import pandas as pd
import requests
import us

from can_tools.scrapers.base import CMU
from can_tools.scrapers.official.base import FederalDashboard

"""
NOTES: 
    some not tallied by region but department (eg: federal entities) -- how to include?

    normally i would make pfizer and moderna just different objects of same class
        but idk if that would work with the setup we have -- so the moderna class is a "child" of the pfizer but has the same methods/attributes 
"""


class CDCVaccinePfizer(FederalDashboard):
    has_location = True
    location_type = "state"
    provider = "cdc"
    query_type = "pfizer"
    source = "https://data.cdc.gov/Vaccinations/COVID-19-Vaccine-Distribution-Allocations-by-Juris/saz5-9hgg"
    crename = {
        "total_pfizer_allocation_first_dose_shipments": CMU(
            category="pfizer_vaccine_first_dose_allocated",
            measurement="cumulative",
            unit="doses",
        ),
        "total_allocation_pfizer_second_dose_shipments": CMU(
            category="pfizer_vaccine_second_dose_allocated",
            measurement="cumulative",
            unit="doses",
        ),
        "doses_allocated_week_of": CMU(
            category="pfizer_vaccine_first_dose_allocated",
            measurement="new_7_day",
            unit="doses",
        ),
        "second_dose_shipment_week_of": CMU(
            category="pfizer_vaccine_second_dose_allocated",
            measurement="new_7_day",
            unit="doses",
        ),
    }

    def fetch(self):
        fetch_urls = {
            "moderna": "https://data.cdc.gov/resource/b7pe-5nws.json",
            "pfizer": "https://data.cdc.gov/resource/saz5-9hgg.json",
        }
        res = requests.get(fetch_urls[self.query_type])

        if not res.ok:
            raise ValueError("could not complete request from url source")
        return res

    def normalize(self, data):
        raw = data.json()
        df = pd.json_normalize(raw).rename(columns={"jurisdiction": "location"})

        # fix column names to match us library convention & remove extra chars
        df["location"] = df["location"].str.replace("*", "").str.replace(" ~", "")
        fix_names = {"U.S. Virgin Islands": "Virgin Islands"}
        df["location"] = df["location"].map(fix_names).fillna(df["location"])

        # use when dataset was last updated as date (this is overriden later for the weekly allocations--but is kept for the total allocations)
        url_time = data.headers["Last-Modified"]
        df["dt"] = pd.to_datetime(url_time, format="%a, %d %b %Y %H:%M:%S GMT").date()

        # replace location names w/ fips codes; keep only locations that have a fips code
        df["location"] = df["location"].map(us.states.mapping("name", "fips"))
        df = df.dropna().reset_index(drop=True)

        # melt into correct format and return
        return self._reshape(df)

    def _reshape(self, data):
        """
        melt data into format for put() function ()
        takes dates embedded in weekly allocation column headers and uses them as dates in the dt column.
        """
        # use these columns for melt (replaces crename.keys())
        colnames = list(data.columns)
        # removes cols we dont want to melt with
        colnames = [e for e in colnames if e not in {"hhs_region", "dt", "location"}]

        out = data.melt(id_vars=["dt", "location"], value_vars=colnames).dropna()

        # temporary column--format values as a date, then replace dt with these vals
        out["dt_str"] = out["variable"]
        # remove date from column names to make usuable as a CMU variable (leave "total" rows as is)
        out.loc[~out["variable"].str.contains("total"), "variable"] = (
            out["variable"].str.strip().str[:-6]
        )

        # rename non matching colnames
        # the weekly column names change a lot — i've chosen the standard based on which was the most frequently used
        fix_first_doses = [
            "first_doses",
            "doses_distribution_week_of",
            "doses_allocated_week",
        ]
        fix_second_doses = [
            "second_dose_shipment",
            "second_doses_shipment",
            "second_dose_week_of",
        ]
        out.loc[
            out["variable"].isin(fix_second_doses), "variable"
        ] = "second_dose_shipment_week_of"
        out.loc[
            out["variable"].isin(fix_first_doses), "variable"
        ] = "doses_allocated_week_of"

        # take the date from in column name and transform into date -- leave 'total' entries unchanged (mark as keep for keep original date)
        out.loc[out["dt_str"].str.contains("total"), "dt_str"] = "keep"
        out.loc[~out["dt_str"].str.contains("total"), "dt_str"] = (
            out["dt_str"].str.strip().str[-5:]
        )
        out["dt_str"] = pd.to_datetime(
            out.dt_str[out["dt_str"] != "keep"], format="%m_%d"
        )
        #"keeps" get transformed to NaT's

        # add year to dt_str depending on the month (if december, 2020, if not december, 2021)
        out["dt_str"] = out["dt_str"].mask(
            out["dt_str"].dt.month == 12,
            out["dt_str"] + pd.DateOffset(years=120),
        )
        out["dt_str"] = out["dt_str"].mask(
            out["dt_str"].dt.month != 12,
            out["dt_str"] + pd.DateOffset(years=121),
        )

        # replace dt with non null dt_str data (replace weekly allocation rows, leave total rows)
        out.loc[out["dt_str"].notnull(), "dt"] = out["dt_str"].dt.date

        out = self.extract_CMU(out, self.crename)
        out["vintage"] = self._retrieve_vintage()
        if out["value"].dtype == object:
            out["value"] = (
                out["value"].str.replace(",", "").str.replace("N/A", "0").astype(int)
            )

        cols_to_keep = [
            "vintage",
            "dt",
            "location",
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


class CDCVaccineModerna(CDCVaccinePfizer):
    """
    subclass of CDCVaccinePfizer, but has exactly the same methods/attributes
    """

    query_type = "moderna"
    source = "https://data.cdc.gov/Vaccinations/COVID-19-Vaccine-Distribution-Allocations-by-Juris/b7pe-5nws"
    crename = {
        "total_moderna_allocation_first_dose_shipments": CMU(
            category="moderna_vaccine_first_dose_allocated",
            measurement="cumulative",
            unit="doses",
        ),
        "total_allocation_moderna_second_dose_shipments": CMU(
            category="moderna_vaccine_second_dose_allocated",
            measurement="cumulative",
            unit="doses",
        ),
        "doses_allocated_week_of": CMU(
            category="moderna_vaccine_first_dose_allocated",
            measurement="new_7_day",
            unit="doses",
        ),
        "second_dose_shipment_week_of": CMU(
            category="moderna_vaccine_second_dose_allocated",
            measurement="new_7_day",
            unit="doses",
        ),
    }
