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
    last_updated: str
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
        self.last_updated = data.headers["Last-Modified"]
        df = pd.json_normalize(raw).rename(columns={"jurisdiction": "location"})

        # fix column names to match us library convention & remove extra chars
        df["location"] = df["location"].str.replace("*", "").str.replace(" ~", "")
        fix_names = {"U.S. Virgin Islands": "Virgin Islands"}
        df["location"] = df["location"].map(fix_names).fillna(df["location"])

        # replace location names w/ fips codes; keep only locations that have a fips code
        df["location"] = df["location"].map(us.states.mapping("name", "fips"))
        df = df.dropna().reset_index(drop=True)

        # melt into correct format and return
        return self._reshape(df)

    def _reshape(self, data):
        """
        melt data into format for put() function ()
        takes dates embedded in weekly allocation column headers and uses them as dates in the dt column.

        Accepts
        -------
            data: pandas.Dataframe()
        Returns: 
            pandas.Dataframe()
        """
        # use these columns for transformation to long format (replaces crename.keys())
        # removes cols we dont want to melt with
        colnames = list(data.columns)
        colnames = [i for i in colnames if i not in {"hhs_region", "location"}]
        out = data.melt(id_vars=["location"], value_vars=colnames).dropna()

        """
            working with the dates:
                for weekly entries: extract dates from column headers ('variable' column)
                for 'total' entries: use when dataset last updated as date 
        """
        #set date column as string version of correct date (see above)
        out["dt"] = out["variable"]
        out.loc[out["variable"].str.contains("total"), "dt"] = self.last_updated
        out.loc[~out["variable"].str.contains("total"), "dt"] = (
            out["dt"].str.strip().str[-5:]
        )

        #convert both string formats to date -- uses temporary column that's later dropped
        out['tmp'] = pd.to_datetime(out['dt'], format='%m_%d', errors='coerce')
        mask = out['tmp'].isnull()
        out.loc[mask, 'tmp'] = pd.to_datetime(out[mask]['dt'], format='%a, %d %b %Y %H:%M:%S GMT',
                                             errors='coerce')
        out["dt"] = out["tmp"]
        out = out.drop(columns={"tmp"})

        # update to current year if year is 1900 (set by default). If december -> 2020, if not december -> 2021
        out["dt"] = out["dt"].mask(
            (out["dt"].dt.month == 12) & (out["dt"].dt.year == 1900),
            out["dt"] + pd.DateOffset(years=120),
        )
        out["dt"] = out["dt"].mask(
            (out["dt"].dt.month != 12) & (out["dt"].dt.year == 1900),
            out["dt"] + pd.DateOffset(years=121),
        )
        out['dt'] = out['dt'].dt.date

        """
            working with variable/mapping:
                standardize variable names (remove date endings, convert to 'standard' string if necessary)
                extract CMU parings
                add vintage, convert dtypes if needed
        """
        # remove date from column names to make usuable as a CMU variable (leave "total" rows as is)
        out.loc[~out["variable"].str.contains("total"), "variable"] = (
            out["variable"].str.strip().str[:-6]
        )

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
        print(out)


        # replace variable column with specified CMU paring, add vintage, convert value column to int
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
