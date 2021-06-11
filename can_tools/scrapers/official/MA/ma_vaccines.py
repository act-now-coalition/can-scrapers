import us
import requests
import re
import pandas as pd
from can_tools.scrapers.official.base import StateDashboard
from can_tools.scrapers import variables
from bs4 import BeautifulSoup as bs


class MassachusettsVaccineDemographics(StateDashboard):

    source = "https://www.mass.gov/info-details/massachusetts-covid-19-vaccination-data-and-updates"

    source_name = "Massachusetts Department of Health"
    location_type = "county"
    state_fips = int(us.states.lookup("Massachusetts").fips)
    has_location = False

    variables = {
        "Fully vaccinated individuals": variables.FULLY_VACCINATED_ALL,
        "Individuals with at least one dose": variables.INITIATING_VACCINATIONS_ALL,
    }

    def _get_url(self):
        # find up-to-date csv from source page
        data = requests.get(self.source)
        soup = bs(data.content, "lxml")
        url = soup.find("a", href=re.compile("covid-19-municipality-vaccination"))
        return "https://www.mass.gov" + url["href"]

    def fetch(self):
        data = requests.get(self._get_url())
        return data.content

    def normalize(self, data: pd.DataFrame) -> pd.DataFrame():

        dfs = []
        # scrape each demographic type then combine
        demos = {
            "Sex - municipality": "sex",
            "Race and ethnicity - muni.": "race",
            "Age - municipality": "age",
        }
        for sheet, demo in demos.items():
            df = (
                pd.read_excel(data, sheet, header=[1])
                .replace("*", 0)
                .rename(
                    columns={"Sex": "sex", "Age Group": "age", "Race/Ethnicity": "race"}
                )
            )

            # sum each town in county to get county total
            df = df.groupby(["County", demo]).sum().reset_index()

            # rename and melt
            df = self._rename_or_add_date_and_location(
                df,
                location_name_column="County",
                timezone="US/Eastern",
            )
            df = self._reshape_variables(
                df, self.variables, skip_columns=[demo], id_vars=[demo]
            )

            # format the demographic values and remove 'Total' rows
            df = df[df[demo] != "Total"]
            if demo in ["race", "sex"]:
                df[demo] = df[demo].str.lower()
            else:
                df[demo] = df[demo].str.replace(" Years", "")
            dfs.append(df)

        out = pd.concat(dfs)
        out = out[out["location_name"] != "Unspecified"]
        return out.replace(
            {
                "75+": "75_plus",
                "multi": "multiple",
                "nh/pi": "pacific_islander",
                "other/unknown": "other",
                "ai/an": "ai_an",
            }
        )
