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
    has_location = False
    location_type = "county"
    state_fips = int(us.states.lookup("Arizona").fips)

    def fetch(self):
        # Set url of downloadable dataset
        url = "https://azdhs.gov/documents/preparedness/epidemiology-disease-control/infectious-disease-epidemiology/novel-coronavirus/vaccine-phases.pdf"

        return camelot.read_pdf(url, pages="2-4", flavor="stream")

    def normalize(self, data) -> pd.DataFrame:
        # Read data into data frames
        table1 = data[0].df
        table2 = data[1].df
        table3 = data[2].df

        # Remove extra header and footer columns
        table1 = table1.iloc[4:-2].reset_index(drop=True)
        table2 = table2.iloc[4:-2].reset_index(drop=True)
        table3 = table3.iloc[6:-7].reset_index(drop=True)

        # Use this if we want to include State PODs, Tribes, CDC Pharmacy Partnership, and ADHS
        # table1 = table1.iloc[4:-1].reset_index(drop=True)
        # table2 = table2.iloc[4:-1].reset_index(drop=True)
        # table3 = table3.iloc[6:-1].reset_index(drop=True)
        # loc_replacer = {"State PODs**": "State PODs", "ADHS‡": "ADHS"}
        # table1 = table1.replace({"location_name": loc_replacer})
        # table3 = table3.replace({"location_name": loc_replacer})
        # table3 = table3.drop([16, 18])
        # table3.at[17, 0] = "CDC Pharmacy Partnership"

        # Rename column names
        table1.columns = [
            "location_name",
            "phase",
            "total_vaccine_doses_administered",
            "ordered",
            "percent_utilized",
            "vaccination_rate",
        ]
        table2.columns = [
            "location_name",
            "phase",
            "ordered",
            "total_vaccine_initiated",
            "total_vaccine_completed",
        ]
        table3.columns = [
            "location_name",
            "pfizer_vaccine_allocated_new_doses",
            "pfizer_vaccine_allocated",
            "moderna_vaccine_allocated_new_doses",
            "moderna_vaccine_allocated",
            "total_vaccine_allocated",
        ]

        # Determine what columns to keep
        table1_cols_to_keep = [
            "location_name",
            "total_vaccine_doses_administered",
        ]
        table2_cols_to_keep = [
            "location_name",
            "total_vaccine_initiated",
            "total_vaccine_completed",
        ]
        table3_cols_to_keep = [
            "location_name",
            "pfizer_vaccine_allocated",
            "moderna_vaccine_allocated",
            "total_vaccine_allocated",
        ]

        # Drop extraneous columns
        table1 = table1.loc[:, table1_cols_to_keep]
        table2 = table2.loc[:, table2_cols_to_keep]
        table3 = table3.loc[:, table3_cols_to_keep]

        # Merge tables
        table1_table2 = pd.merge(table1, table2, how="outer", on="location_name")
        df = pd.merge(table1_table2, table3, how="outer", on="location_name")

        # Create dictionary for columns to map
        crename = {
            "total_vaccine_doses_administered": CMU(
                category="total_vaccine_doses_administered",
                measurement="cumulative",
                unit="doses",
            ),
            "total_vaccine_initiated": CMU(
                category="total_vaccine_initiated",
                measurement="cumulative",
                unit="people",
            ),
            "total_vaccine_completed": CMU(
                category="total_vaccine_completed",
                measurement="cumulative",
                unit="people",
            ),
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
