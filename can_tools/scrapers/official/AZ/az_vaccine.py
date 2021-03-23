from can_tools.scrapers.variables import (
    FULLY_VACCINATED_ALL,
    INITIATING_VACCINATIONS_ALL,
    TOTAL_DOSES_ADMINISTERED_ALL,
)
import camelot
import pandas as pd
import us

from can_tools.scrapers.base import CMU
from can_tools.scrapers.official.base import StateDashboard, TableauDashboard


class NoTableauMapFiltersFoundError(Exception):
    """Raised if no Tableau Map Filters are found."""


class ArizonaVaccineCounty(TableauDashboard):
    """
    Fetch county level covid data from Arizona's Tableau dashboard
    """

    baseurl = "https://tableau.azdhs.gov"
    viewPath = "VaccineDashboard/Vaccineadministrationdata"

    # Initlze
    source = "https://www.azdhs.gov/preparedness/epidemiology-disease-control/infectious-disease-epidemiology/covid-19/dashboards/index.php"
    source_name = "Arizona Department Of Health Services"
    has_location = False
    location_type = "county"
    state_fips = int(us.states.lookup("Arizona").fips)
    location_name_col = "location_name"
    timezone = "US/Mountain"

    filterFunctionName = (
        "[federated.1rsrm840sp0wgc11a5yw61x1aht3].[Calculation_624592978064752643]~s0"
    )
    counties = [
        ["APACHE", 4001],
        ["COCHISE", 4003],
        ["COCONINO", 4005],
        ["GILA", 4007],
        ["GRAHAM", 4009],
        ["GREENLEE", 4011],
        ["LA PAZ", 4012],
        ["MARICOPA", 4013],
        ["MOHAVE", 4015],
        ["NAVAJO", 4017],
        ["PIMA", 4019],
        ["PINAL", 4021],
        ["SANTA CRUZ", 4023],
        ["YAVAPAI", 4025],
        ["YUMA", 4027],
    ]

    cmus = {
        "total_vaccine_initiated": INITIATING_VACCINATIONS_ALL,
        "total_vaccine_completed": FULLY_VACCINATED_ALL,
        "total_doses_administered": TOTAL_DOSES_ADMINISTERED_ALL,
    }

    def fetch(self):
        dfs = []
        for county_name, fips in self.counties:
            self.filterFunctionValue = county_name
            county_data = self.get_tableau_view()
            data = {
                "total_doses_administered": self._get_doses_administered(county_data),
                "total_vaccine_initiated": self._get_vaccines_initiated(county_data),
                "total_vaccine_completed": self._get_vaccines_completed(county_data),
                "location": fips,
                "location_name": county_name,
            }
            dfs.append(pd.DataFrame(data))

        # Concat the dfs
        output_df = pd.concat(dfs, axis=0, ignore_index=True)
        return output_df

    def _get_doses_administered(self, data):
        return data["Number of Doses"]["AGG(Number of Doses)-alias"]

    def _get_vaccines_completed(self, data):
        return data["Complete Vaccine Series"][
            "AGG(Total number of people (LOD))-alias"
        ]

    def _get_vaccines_initiated(self, data):
        return data[" County using admin county"]["AGG(Number of People)-alias"]


class ArizonaVaccineCountyAllocated(StateDashboard):
    """
    Fetch county level Covid-19 vaccination data from official state of Arizona PDF
    """

    source = "https://directorsblog.health.azdhs.gov/covid-19-vaccine-surveillance/"
    source_name = "Arizona Department Of Health Services"
    has_location = False
    location_type = "county"
    state_fips = int(us.states.lookup("Arizona").fips)

    def fetch(self):
        # Set url of downloadable dataset
        url = "https://azdhs.gov/documents/preparedness/epidemiology-disease-control/infectious-disease-epidemiology/novel-coronavirus/vaccine-phases.pdf"

        return camelot.read_pdf(url, pages="2", flavor="stream")

    def normalize(self, data) -> pd.DataFrame:
        # adding values to this array will cause all rows where location_name is
        # in this array to be removed
        non_counties = ["Tribes"]
        # Sanity check how many tables we got back
        if len(data) > 1:
            raise ValueError("more tables returned than expected value")

        # Read data into data frames
        df = data[0].df

        # Remove extra header and footer columns
        df = df.iloc[6:-7].reset_index(drop=True)

        # Use this if we want to include State PODs, Tribes, CDC Pharmacy Partnership, and ADHS
        # loc_replacer = {"State PODs**": "State PODs", "ADHS‡": "ADHS"}
        # df = df.replace({"location_name": loc_replacer})
        # df = df.drop([16, 18])
        # df.at[17, 0] = "CDC Pharmacy Partnership"

        # Rename column names
        df.columns = [
            "location_name",
            "pfizer_vaccine_allocated_new_doses",
            "pfizer_vaccine_allocated",
            "moderna_vaccine_allocated_new_doses",
            "moderna_vaccine_allocated",
            "janssen_vaccine_new_doses",
            "janssen_vaccine_allocated",
            "total_vaccine_allocated",
        ]

        # Determine what columns to keep
        cols_to_keep = [
            "location_name",
            "pfizer_vaccine_allocated",
            "moderna_vaccine_allocated",
            "janssen_vaccine_allocated",
            "total_vaccine_allocated",
        ]

        # Drop extraneous columns
        df = df.loc[:, cols_to_keep]

        # Create dictionary for columns to map
        crename = {
            "moderna_vaccine_allocated": CMU(
                category="moderna_vaccine_allocated",
                measurement="cumulative",
                unit="doses",
            ),
            "pfizer_vaccine_allocated": CMU(
                category="pfizer_vaccine_allocated",
                measurement="cumulative",
                unit="doses",
            ),
            "janssen_vaccine_allocated": CMU(
                category="janssen_vaccine_allocated",
                measurement="cumulative",
                unit="doses",
            ),
            "total_vaccine_allocated": CMU(
                category="total_vaccine_allocated",
                measurement="cumulative",
                unit="doses",
            ),
        }

        # Move things into long format
        df = df.melt(id_vars=["location_name"], value_vars=crename.keys()).dropna()

        # Determine the category of each observation
        out = self.extract_CMU(df, crename)

        # Convert value columns, remove commas
        out.loc[:, "value"] = pd.to_numeric(out["value"].str.replace(",", ""))

        # Add rows that don't change
        out["vintage"] = self._retrieve_vintage()
        out["dt"] = self._retrieve_dt("US/Arizona")

        return out.query("location_name not in @non_counties")

    def validate(self, df, df_hist) -> bool:
        "Quality is free, but only to those who are willing to pay heavily for it. - DeMarco and Lister"
        return True
