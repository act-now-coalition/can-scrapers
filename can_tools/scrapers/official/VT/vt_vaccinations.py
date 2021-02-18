import pandas as pd
import us

from can_tools.scrapers import CMU
from can_tools.scrapers.official.base import ArcGIS

pd.options.mode.chained_assignment = None  # Avoid unnessacary SettingWithCopy warning


class VermontCountyVaccine(ArcGIS):
    ARCGIS_ID = "YKJ5JtnaPQ2jDbX8"
    has_location = False
    location_type = "county"
    state_fips = int(us.states.lookup("Vermont").fips)
    source = "https://www.healthvermont.gov/covid-19/vaccine/covid-19-vaccine-dashboard"
    source_name = "Vermont Department of Health"
    var_columns = [
        "dose_1_male",
        "dose_1_female",
        "dose_1_sex_unknown",
        "dose_2_male",
        "dose_2_female",
        "dose_2_sex_unknown",
    ]

    def fetch(self):
        return self.get_all_jsons("VT_COVID_Vaccine_Data", 0, "")

    def normalize(self, data):
        data = (
            self.arcgis_jsons_to_df(data)
            .fillna(0)
            .rename(columns={"County": "location_name"})
        )
        data = self._get_clean_data(data)
        newest_date = data["dt"].max()

        # get cumulative data
        df = data.melt(
            id_vars=["dt", "location_name"], value_vars=self.var_columns
        ).dropna()

        # sum total values by county for first and second dose
        # NOTE: This currently uses a sum to get the total number of
        #       vaccines administered -- This depends on the fact
        #       that we're only working with sex for now
        df1 = (
            df.query('variable.str.contains("dose_1")')
            .groupby("location_name", as_index=False)["value"]
            .sum()
        )
        df1["category"] = "total_vaccine_initiated"
        df2 = (
            df.query('variable.str.contains("dose_2")')
            .groupby("location_name", as_index=False)["value"]
            .sum()
        )
        df2["category"] = "total_vaccine_completed"

        # combine dfs and fill needed columns
        df = pd.concat(
            [
                df1,
                df2,
            ],
            axis=0,
            ignore_index=True,
        )
        cumulative_df = self._populate_cols(df, newest_date)

        # get weekly snapshots
        # create "total" 1st and 2nd dose columns by week
        weekly_df = self._get_clean_data(data)
        weekly_df["dose_1"] = (
            weekly_df["dose_1_male"]
            + weekly_df["dose_1_female"]
            + weekly_df["dose_1_sex_unknown"]
        )
        weekly_df["dose_2"] = (
            weekly_df["dose_2_male"]
            + weekly_df["dose_2_female"]
            + weekly_df["dose_2_sex_unknown"]
        )

        crename = {
            "dose_1": CMU(
                category="total_vaccine_initiated",
                measurement="new_7_day",
                unit="people",
            ),
            "dose_2": CMU(
                category="total_vaccine_completed",
                measurement="new_7_day",
                unit="people",
            ),
            "dose_1_male": CMU(
                category="total_vaccine_initiated",
                measurement="new_7_day",
                unit="people",
                sex="male",
            ),
            "dose_2_male": CMU(
                category="total_vaccine_completed",
                measurement="new_7_day",
                unit="people",
                sex="male",
            ),
            "dose_1_female": CMU(
                category="total_vaccine_initiated",
                measurement="new_7_day",
                unit="people",
                sex="female",
            ),
            "dose_2_female": CMU(
                category="total_vaccine_completed",
                measurement="new_7_day",
                unit="people",
                sex="female",
            ),
            "dose_1_sex_unknown": CMU(
                category="total_vaccine_initiated",
                measurement="new_7_day",
                unit="people",
                sex="unknown",
            ),
            "dose_2_sex_unknown": CMU(
                category="total_vaccine_completed",
                measurement="new_7_day",
                unit="people",
                sex="unknown",
            ),
        }

        weekly_df = weekly_df.melt(
            id_vars=["location_name", "dt"], value_vars=crename.keys()
        ).dropna()
        weekly_df["value"] = weekly_df["value"].astype(int)
        weekly_df["vintage"] = self._retrieve_vintage()

        # Extract category information and add other variable context
        weekly_df = self.extract_CMU(weekly_df, crename)
        weekly_df = weekly_df.drop(columns={"variable"})

        return pd.concat([cumulative_df, weekly_df], ignore_index=True)

    def _get_clean_data(self, data):
        """
        clean data -- rename columns and reformat date column

        accepts: pandas.Dataframe
        returns: pandas.Dataframe
        """
        data.columns = [x.lower() for x in list(data)]
        data = data.query("location_name != 'Unknown/Other'")

        # keep second date
        data["dt"] = data["dates"].str.split("-").str[1]
        data["dt"] = pd.to_datetime(data["dt"], errors="coerce")

        return data

    def _populate_cols(self, data, dt):
        """
        create and populate necessary columns for put() function. format value to numeric

        accepts: pandas.Dataframe
        returns: pandas.Dataframe
        """
        df = data.assign(
            measurement="cumulative",
            unit="people",
            age="all",
            race="all",
            ethnicity="all",
            sex="all",
        )

        df["dt"] = dt
        df["vintage"] = self._retrieve_vintage()
        df.loc[:, "value"] = pd.to_numeric(df["value"])

        return df


class VermontStateVaccine(VermontCountyVaccine):
    has_location = True
    location_type = "state"

    def normalize(self, data):
        data = (
            self.arcgis_jsons_to_df(data)
            .fillna(0)
            .rename(columns={"County": "location_name"})
        )
        df = self._get_clean_data(data)
        newest_date = df["dt"].max()

        # transform to long -- keep only dose 1 and 2 for each sex
        df = df.melt(
            id_vars=["dt", "location_name"], value_vars=self.var_columns
        ).dropna()

        # get cumulative state values (sum all rows for each dose)
        first_dose = df.loc[df["variable"].str.contains("dose_1"), "value"].sum()
        second_dose = df.loc[df["variable"].str.contains("dose_2"), "value"].sum()

        # combine values into a df and add necessary columns
        d = [
            {"value": first_dose, "category": "total_vaccine_initiated"},
            {"value": second_dose, "category": "total_vaccine_completed"},
        ]
        df = pd.DataFrame(d)
        df = self._populate_cols(df, newest_date)

        df["location"] = self.state_fips
        return df
