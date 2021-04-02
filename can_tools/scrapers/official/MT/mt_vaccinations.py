import pandas as pd
import us

from can_tools.scrapers import ScraperVariable
from can_tools.scrapers.official.base import ArcGIS


class MontanaCountyVaccine(ArcGIS):
    ARCGIS_ID = "qnjIrwR8z5Izc0ij"
    has_location = True
    location_type = "county"
    state_fips = int(us.states.lookup("Montana").fips)
    source = "https://montana.maps.arcgis.com/apps/MapSeries/index.html?appid=7c34f3412536439491adcc2103421d4b"
    source_name = "Montana Department of Health & Human Services"
    crename = {
        "Dose_1": ScraperVariable(
            category="total_vaccine_initiated",
            measurement="cumulative",
            unit="people",
        ),
        "Dose_2": ScraperVariable(
            category="total_vaccine_completed",
            measurement="cumulative",
            unit="people",
        ),
    }

    def fetch(self):
        return self.get_all_jsons("COVID_Vaccination_PRD_View", 0, "")

    def normalize(self, data):
        df = (
            self.arcgis_jsons_to_df(data)
            .fillna(0)
            .rename(columns={"ALLFIPS": "location", "Date_Reported": "dt"})
        )

        # Capitalize start of words and replace wrong names
        df.loc[:, "location"] = (
            df.loc[:, "location"]
            .str.title()
            .replace({"Lewis & Clark": "Lewis and Clark", "Mccone": "McCone"})
        )

        df["dt"] = df["dt"].map(self._esri_ts_to_dt)

        # this matches the values in the dashboard

        return self._transform_df(df)

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
            id_vars=["dt", loc_col_type], value_vars=self.crename.keys()
        ).dropna()
        out.loc[:, "value"] = pd.to_numeric(out["value"])
        out = self.extract_scraper_variables(out, self.crename)
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
    crename = {
        "Total_Montanans_Immunized": ScraperVariable(
            category="total_vaccine_completed",
            measurement="cumulative",
            unit="people",
        ),
        "Total_Doses_Administered": ScraperVariable(
            category="total_vaccine_doses_administered",
            measurement="cumulative",
            unit="doses",
        ),
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

        return self._transform_df(df)
