import pandas as pd
from tabula import read_pdf
import us

from can_tools.scrapers import CMU
from can_tools.scrapers.official.base import StateDashboard


class FloridaCountyVaccine(StateDashboard):
    has_location = False
    source = "https://floridahealthcovid19.gov/#latest-stats"
    location_type = "county"
    state_fips = int(us.states.lookup("Florida").fips)

    def fetch(self):
        fetch_url = "http://ww11.doh.state.fl.us/comm/_partners/covid19_report_archive/vaccine/vaccine_report_latest.pdf"
        """ area is the location of table in pdf by distance from [top, left, top + height, left + width] in units of pixels (inches*72)
            see https://stackoverflow.com/a/61097723/14034347
        """
        return read_pdf(
            fetch_url,
            pages=2,
            area=[134, 77, 1172.16, 792],
            pandas_options={"dtype": str},
        )

    def normalize(self, data):
        df = pd.concat(data).rename(columns={"County of residence": "location_name"})

        # Ignore data from unknown region (no fips code) and fix naming convention for problem counties
        df = df[
            (df["location_name"] != "Unknown") & (df["location_name"] != "Out-Of-State")
        ]
        df.loc[df["location_name"] == "Desoto", "location_name"] = "DeSoto"
        df.loc[df["location_name"] == "Dade", "location_name"] = "Miami-Dade"

        crename = {
            "First dose": CMU(
                category="total_vaccine_initiated",
                measurement="new",
                unit="people",
            ),
            "Series\rcomplete": CMU(
                category="total_vaccine_completed",
                measurement="new",
                unit="people",
            ),
            "Total people\rvaccinated": CMU(
                category="total_vaccine_doses_administered",
                measurement="new",
                unit="doses",
            ),
            "First dose.1": CMU(
                category="total_vaccine_initiated",
                measurement="cumulative",
                unit="people",
            ),
            "Series\rcomplete.1": CMU(
                category="total_vaccine_completed",
                measurement="cumulative",
                unit="people",
            ),
            "Total people\rvaccinated.1": CMU(
                category="total_vaccine_doses_administered",
                measurement="cumulative",
                unit="doses",
            ),
        }
        return self._reshape(df, crename)

    def _reshape(self, data, _map):
        out = data.melt(id_vars=["location_name"], value_vars=_map.keys()).dropna()

        out = self.extract_CMU(out, _map)
        out.loc[:, "value"] = pd.to_numeric(out["value"].str.replace(",", ""))
        out["vintage"] = self._retrieve_vintage()
        out["dt"] = self._retrieve_dtm1d("US/Eastern")

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
