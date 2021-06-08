import pandas as pd
import us
import numpy as np
from can_tools.scrapers import variables
from can_tools.scrapers.official.base import ArcGIS


class LAVaccineCounty(ArcGIS):
    provider = "state"
    ARCGIS_ID = "O5K6bb5dZVZcTo5M"
    has_location = True
    location_type = "county"
    state_fips = int(us.states.lookup("Louisiana").fips)
    source = "https://ladhh.maps.arcgis.com/apps/opsdashboard/index.html#/7b2a0703a129451eaaf9046c213331ed"
    source_name = "Louisiana Department of Health"

    def fetch(self):
        return self.get_all_jsons("Vaccinations_by_Race_by_Parish", 0, 5)

    def normalize(self, data):
        df = self.arcgis_jsons_to_df(data)
        df = df.rename(
            columns={
                "PFIPS": "location",
            }
        )
        df["dt"] = self._retrieve_dt("US/Central")

        melted = df.melt(
            id_vars=["dt", "location"], value_vars=["SeriesInt", "SeriesComp"]
        )

        crename = {
            "SeriesInt": variables.INITIATING_VACCINATIONS_ALL,
            "SeriesComp": variables.FULLY_VACCINATED_ALL,
        }

        result = self.extract_CMU(melted, crename)

        result["vintage"] = self._retrieve_vintage()

        return result.drop(["variable"], axis="columns")


class LAVaccineCountyDemographics(LAVaccineCounty):
    variables = {
        "PercInt": variables.INITIATING_VACCINATIONS_ALL,
        "PercComp": variables.FULLY_VACCINATED_ALL,
    }

    def normalize(self, data):
        data = self.arcgis_jsons_to_df(data)
        demographcis = {
            "sex": ["Female", "Male", "SexUnk"],
            "race": ["Black", "White", "RaceUnk", "Other"],
            "age": [
                "5to17",
                "18to29",
                "30to39",
                "40to49",
                "50to59",
                "60to69",
                "70plus",
            ],
        }

        # loop through each demographic and each dose type the concat together
        dfs = []
        for demo, cols in demographcis.items():
            for dose, dose_prefix in {
                "SeriesInt": "PercInt_",
                "SeriesComp": "PercComp_",
            }.items():
                # get location and total dose value
                columns = ["PFIPS", dose]
                # add the prefix (PercInt_ or PercComp_) to find the corresponding column
                var_cols = [(dose_prefix + c) for c in cols]
                columns.extend(var_cols)
                df = data.loc[:, columns]

                # divide percentages by 100 then multiply by the total doses
                # to get # of individuals by demographic
                df[var_cols] = df[var_cols].multiply(0.01 * df[dose], axis="index")

                # melt and split variable and demographic column into two + extract variables
                df = df.drop(columns={dose}).melt(id_vars=["PFIPS"])
                df[["variable", demo]] = df["variable"].str.split("_", expand=True)
                df = self.extract_CMU(df=df, cmu=self.variables, skip_columns=[demo])

                # demographic value formatting
                df[demo] = df[demo].str.lower()
                if demo == "age":
                    df[demo] = df[demo].str.replace("to", "-")
                dfs.append(df)

        return (
            pd.concat(dfs)
            .assign(
                value=lambda x: x["value"].astype(int),
                dt=self._retrieve_dt("US/Eastern"),
                vintage=self._retrieve_vintage(),
            )
            .replace(
                {
                    "sexunk": "unknown",
                    "70plus": "70_plus",
                    "raceunk": "unknown",
                }
            )
            .drop(columns={"variable"})
            .rename(columns={'PFIPS': 'location'})
        )
