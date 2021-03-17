from typing import Dict, Optional


import pandas as pd
import camelot
import requests
import us

from can_tools.scrapers import CMU
from can_tools.scrapers.official.base import StateDashboard


def normalize_table(
    df: pd.DataFrame,
    variable_map: Dict[str, CMU],
    vintage: pd.Timestamp,
    location_name_column: str,
    date: Optional[pd.Timestamp] = None,
    date_column: Optional[str] = None,
) -> pd.DataFrame:
    # county names (converted to title case)
    df = df.copy()
    df["location_name"] = df[self.location_name_col].str.title()

    if date_column:
        df = df.rename(columns={date_column: "dt"})
        df["dt"] = pd.to_datetime(df["dt"])
    else:
        assert date
        df["dt"] = date

    # parse out data columns
    value_cols = list(set(df.columns) & set(self.cmus.keys()))
    assert len(value_cols) == len(self.cmus)

    output_df = (
        df.melt(id_vars=["location_name"], value_vars=value_cols)
        .dropna()
        .assign(
            vintage=vintage,
            value=lambda x: pd.to_numeric(x["value"].astype(str).str.replace(",", "")),
        )
        .pipe(self.extract_CMU, cmu=self.cmus)
        .drop(["variable"], axis=1)
    )

    return output_df


class MSCountyVaccine(StateDashboard):
    has_location = False
    source = "https://msdh.ms.gov/msdhsite/_static/resources/12130.pdf"
    location_type = "county"
    state_fips = int(us.states.lookup("Mississippi").fips)
    fetch_url = "https://msdh.ms.gov/msdhsite/_static/resources/12130.pdf"
    source_name = "Mississippi State Department of Health"

    variable_map = {
        "People Receiving at least One Dose": CMU(
            category="total_vaccine_initiated",
            measurement="cumulative",
            unit="people",
        ),
        "People Fully Vaccinated": CMU(
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

    def normalize(self, data):
        # Clean up dataframe from PDF.
        data = data[0].df
        header = data.iloc[1, :].reset_index(drop=True)
        data = data.iloc[2:].reset_index(drop=True)
        data.columns = header.to_list()

        data = self._rename_or_add_date_and_location(
            data,
            location_name_column="County of Residence",
            location_names_to_drop=["Total"],
            timezone="US/Central",
        )
        data = self._reshape_variables(data, self.variable_map)
        data = data.replace({"location_name": {"Desoto": "DeSoto"}})
        return data
