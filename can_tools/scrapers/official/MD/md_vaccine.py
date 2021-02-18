import pandas as pd

import us

from can_tools.scrapers import CMU, DatasetBase
from can_tools.scrapers.official.base import ArcGIS


class MarylandCountyVaccines(ArcGIS, DatasetBase):
    """
    Fetch county level covid data from Maryland's ARCGIS dashboard
    """

    ARCGIS_ID = "njFNhDsUCentVYJW"
    has_location = False
    location_type = "county"
    state_fips = int(us.states.lookup("Maryland").fips)
    source = "https://coronavirus.maryland.gov/#Vaccine"
    source_name = "Maryland Department of Health"

    def fetch(self):
        return self.get_all_jsons("MD_COVID19_VaccinationByCounty", 0, "")

    def normalize(self, data):
        df = self.arcgis_jsons_to_df(data)
        df.columns = [x.lower() for x in list(df)]
        df = df.rename(columns={"county": "location_name"}).query(
            "location_name != 'Unknown'"
        )

        crename = {
            "firstdose": CMU(
                category="total_vaccine_initiated",
                measurement="cumulative",
                unit="people",
            ),
            "seconddose": CMU(
                category="total_vaccine_completed",
                measurement="cumulative",
                unit="people",
            ),
            "percentfirstdose": CMU(
                category="total_vaccine_initiated",
                measurement="current",
                unit="percentage",
            ),
            "percentseconddose": CMU(
                category="total_vaccine_completed",
                measurement="current",
                unit="percentage",
            ),
        }

        out = (
            df.melt(id_vars=["location_name"], value_vars=crename.keys())
            .assign(
                dt=self._retrieve_dt("US/Eastern"), vintage=self._retrieve_vintage()
            )
            .dropna()
        )
        out.loc[:, "value"] = pd.to_numeric(out["value"])

        # Extract category information and add other variable context
        out = self.extract_CMU(out, crename)

        cols_to_keep = [
            "vintage",
            "dt",
            "location_name",
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
