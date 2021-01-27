import pandas as pd
import us

from can_tools.scrapers.base import CMU
from can_tools.scrapers.official.base import ArcGIS


class CASanDiegoVaccine(ArcGIS):
    provider = "county"
    ARCGIS_ID = "1vIhDJwtG5eNmiqX"
    has_location = True
    location_type = "county"
    state_fips = int(us.states.lookup("California").fips)
    county_fips = 73
    source = (
        "https://sdcounty.maps.arcgis.com/apps/opsdashboard/index.html"
        "#/c0f4b16356b840478dfdd50d1630ff2a"
    )

    def fetch(self):
        return self.get_all_jsons("Vaccination_Demographics", 0, 1)

    def normalize_group(self, df, demographic, dgroup, group_rename, cmu):
        keep_vals = list(group_rename.keys())
        foo = df.query("Category == @demographic &" "Join_Name in @keep_vals").rename(
            columns={"Join_Name": dgroup}
        )
        foo.loc[:, dgroup] = foo.loc[:, dgroup].str.strip()
        foo = foo.replace({dgroup: group_rename})

        # Keep all but current demographic info
        all_demographics = ["age", "race", "ethnicity", "sex"]
        all_demographics.remove(dgroup)
        cmu_cols = ["category", "measurement", "unit"] + all_demographics
        foo = self.extract_CMU(foo, cmu, columns=cmu_cols)

        # Drop Category/variable
        foo = foo.drop(["Category", "variable"], axis="columns")

        return foo

    def normalize(self, data):
        # Dump jsons into a df
        df = self.arcgis_jsons_to_df(data).rename(columns={"Catefory": "Category"})

        # Set dt and location
        df["dt"] = df["Update_Date"].map(self._esri_ts_to_dt)
        df["location"] = self.state_fips * 1000 + self.county_fips

        # Reshape
        df = df.melt(
            id_vars=["dt", "location", "Category", "Join_Name"], value_vars=["Count_"]
        )

        # Work with each subgroup of data
        crename = {
            "Count_": CMU(
                category="total_vaccine_initiated",
                measurement="cumulative",
                unit="people",
            )
        }

        age_replace = {
            "0-9 years": "0-9",
            "10-19 years": "10-19",
            "20-29 years": "20-29",
            "30-39 years": "30-39",
            "40-49 years": "40-49",
            "50-59 years": "50-59",
            "60-69 years": "60-69",
            "70-79 years": "70-79",
            "80+ years": "80_plus",
            "Age Unknown": "unknown",
        }
        age_df = self.normalize_group(df, "Age Groups", "age", age_replace, crename)

        gender_replace = {
            "Female": "female",
            "Male": "male",
            "Gender Unknown": "unknown",
        }
        gender_df = self.normalize_group(df, "Gender", "sex", gender_replace, crename)

        race_replace = {
            "American Indian or Alaska Native": "ai_an",
            "Asian": "asian",
            "Black or African-American": "black",
            "Native Hawaiian or Other Pacific Islander": "pacific_islander",
            "Other Race": "other",
            "White": "white",
        }
        race_col = [c for c in df["Category"].unique() if "race" in c.lower()][0]
        race_df = self.normalize_group(df, race_col, "race", race_replace, crename)

        ethnicity_replace = {"Hispanic or Latino": "hispanic"}
        eth_col = [c for c in df["Category"].unique() if "ethnicity" in c.lower()][0]
        eth_df = self.normalize_group(
            df, eth_col, "ethnicity", ethnicity_replace, crename
        )

        # Totals
        total_crename = {
            "DosesShipped": CMU(
                category="total_vaccine_distributed",
                measurement="cumulative",
                unit="doses",
            ),
            "AdministeredDoses": CMU(
                category="total_vaccine_doses_administered",
                measurement="cumulative",
                unit="doses",
            ),
            "IndividualsVacc": CMU(
                category="total_vaccine_initiated",
                measurement="cumulative",
                unit="people",
            ),
            "FullyVacc": CMU(
                category="total_vaccine_completed",
                measurement="cumulative",
                unit="people",
            ),
        }
        total_cols = [
            "IndividualsVacc",
            "FullyVacc",
            "DosesShipped",
            "AdministeredDoses",
        ]
        total_temp = df.query("(Category in @total_cols) & (variable == 'Count_')")
        total_df = self.extract_CMU(
            total_temp, total_crename, var_name="Join_Name"
        ).drop(["Category", "Join_Name", "variable"], axis="columns")

        out = pd.concat(
            [age_df, gender_df, race_df, eth_df, total_df], axis=0, ignore_index=True
        ).dropna()
        out["vintage"] = self._retrieve_vintage()

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
