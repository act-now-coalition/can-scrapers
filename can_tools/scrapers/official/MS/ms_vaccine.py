from typing import Dict, Optional

import camelot
import us

from can_tools.scrapers import CMU
from can_tools.scrapers.official.base import StateDashboard


class MSCountyVaccine(StateDashboard):
    has_location = False
    source = "https://msdh.ms.gov/msdhsite/_static/resources/12130.pdf"
    location_type = "county"
    state_fips = int(us.states.lookup("Mississippi").fips)
    fetch_url = "https://msdh.ms.gov/msdhsite/_static/resources/12130.pdf"
    source_name = "Mississippi State Department of Health"

    variable_map = {
        "People Receiving at least One Dose**": CMU(
            category="total_vaccine_initiated",
            measurement="cumulative",
            unit="people",
        ),
        "People Fully Vaccinated***": CMU(
            category="total_vaccine_completed",
            measurement="cumulative",
            unit="people",
        ),
        "Total Doses Administered": CMU(
            category="total_vaccine_doses_administered",
            measurement="cumulative",
            unit="doses",
        ),
    }

    def fetch(self):
        return camelot.read_pdf(self.fetch_url, pages="2", flavor="stream")

    def normalize(self, raw):
        # Clean up dataframe from PDF.
        data = raw[1].df
        # find header
        header_loc = data.index[data.iloc[:, 0] == "County of Residence"].values[0]
        header = data.loc[[header_loc]].iloc[0]
        # grab all data from after the header
        data = data.iloc[header_loc + 1 :, :].reset_index(drop=True)
        data.columns = header.to_list()

        data = self._rename_or_add_date_and_location(
            data,
            location_name_column="County of Residence",
            location_names_to_drop=["Total", "Out of State"],
            timezone="US/Central",
        )
        data = self._reshape_variables(data, self.variable_map)
        data = data.replace({"location_name": {"Desoto": "DeSoto"}})
        return data.dropna(subset=["value"])
