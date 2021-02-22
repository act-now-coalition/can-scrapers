import camelot
import pandas as pd
import us

from can_tools.scrapers.base import CMU
from can_tools.scrapers.official.base import StateDashboard


class ArizonaVaccineCounty(StateDashboard):
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
            "total_vaccine_allocated",
        ]

        # Determine what columns to keep
        cols_to_keep = [
            "location_name",
            "pfizer_vaccine_allocated",
            "moderna_vaccine_allocated",
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

        return out

    def validate(self, df, df_hist) -> bool:
        "Quality is free, but only to those who are willing to pay heavily for it. - DeMarco and Lister"
        return True
