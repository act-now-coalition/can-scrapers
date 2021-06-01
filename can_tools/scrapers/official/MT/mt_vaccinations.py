import pandas as pd
import us

from can_tools.scrapers import variables
from can_tools.scrapers.official.base import ArcGIS


class MontanaCountyVaccine(ArcGIS):
    ARCGIS_ID = "qnjIrwR8z5Izc0ij"
    has_location = False
    location_type = "county"
    state_fips = int(us.states.lookup("Montana").fips)
    source = "https://montana.maps.arcgis.com/apps/MapSeries/index.html?appid=7c34f3412536439491adcc2103421d4b"
    source_name = "Montana Department of Health & Human Services"
    variables = {
        "Dose_1": variables.INITIATING_VACCINATIONS_ALL,
        "Fully_Vaxed": variables.FULLY_VACCINATED_ALL,
        "Total_Doses_Admin": variables.TOTAL_DOSES_ADMINISTERED_ALL,
    }

    def fetch(self):
        return self.get_all_jsons("COVID_Vaccination_PRD_View", 0, "")

    def normalize(self, data):
        df = (
            self.arcgis_jsons_to_df(data)
            .fillna(0)
            .rename(columns={"NAME": "location_name", "Date_Reported": "dt"})
        )

        # Capitalize start of words and replace wrong names
        df.loc[:, "location_name"] = (
            df.loc[:, "location_name"]
            .str.title()
            .replace({"Lewis & Clark": "Lewis and Clark", "Mccone": "McCone"})
        )

        df["dt"] = df["dt"].map(self._esri_ts_to_dt)

        # this matches the values in the dashboard

        df = self._transform_df(df)
        return df[df["location_name"] != "Montana"]

    def _transform_df(self, data):
        """
        transform pd.Dataframe from wide to long
        add vintage timestamp
        select columns to return
        """
        # specify if has FIPS or not
        if self.has_location:
            loc_col_type = "location"
        elif not self.has_location:
            loc_col_type = "location_name"

        out = data.melt(
            id_vars=["dt", loc_col_type], value_vars=self.variables.keys()
        ).dropna()
        out.loc[:, "value"] = pd.to_numeric(out["value"])
        out = self.extract_CMU(out, self.variables)
        out["vintage"] = self._retrieve_vintage()

        cols_to_keep = [
            "vintage",
            "dt",
            loc_col_type,
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


class MontanaStateVaccine(MontanaCountyVaccine):
    location_type = "state"
    has_location = True
    variables = {
        "Total_Montanans_Immunized": variables.FULLY_VACCINATED_ALL,
        "Total_Doses_Administered": variables.TOTAL_DOSES_ADMINISTERED_ALL,
    }

    def fetch(self):
        return self.get_all_jsons("COVID_Vaccination_PRD_View", 1, "")

    def normalize(self, data):
        df = (
            self.arcgis_jsons_to_df(data)
            .fillna(0)
            .rename(columns={"Report_Date": "dt"})
        )
        df["dt"] = df["dt"].map(self._esri_ts_to_dt)
        df["location"] = self.state_fips

        out = self._transform_df(df)

        # this scraper has some duplicates. Drop them here
        return out.drop_duplicates(
            subset=[
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
            ],
            keep="last",
        )
