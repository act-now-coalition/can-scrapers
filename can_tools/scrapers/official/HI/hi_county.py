import json
import re

import pandas as pd
import requests
import us
from bs4 import BeautifulSoup

from can_tools.scrapers import variables as v
from can_tools.scrapers.official.base import TableauDashboard
from can_tools.scrapers.util import requests_retry_session
from tableauscraper import TableauScraper


class HawaiiVaccineCounty(TableauDashboard):
    has_location = False
    source = "https://health.hawaii.gov/coronavirusdisease2019/what-you-should-know/current-situation-in-hawaii/#vaccine"
    source_name = (
        "State of Hawai'i - Department of Health"
        "Disease Outbreak Control Division | COVID-19"
    )
    location_type = "county"
    state_fips = int(us.states.lookup("Hawaii").fips)
    fetch_url = "https://dohdocdeas.doh.hawaii.gov/t/DOCD/views/HawaiiCOVID-19-VaccinationDashboard/VACCINESBYCOUNTY"

    variables = {
        "Initiating": v.INITIATING_VACCINATIONS_ALL,
        "Completing": v.FULLY_VACCINATED_ALL,
        "Received 3rd Dose": v.PEOPLE_VACCINATED_ADDITIONAL_DOSE,
    }

    def fetch(self) -> pd.DataFrame:
        engine = TableauScraper()
        engine.loads(self.fetch_url)
        workbook = engine.getWorkbook()
        return workbook.getWorksheet("Updated County Progress").data

    def normalize(self, data):
        cols = {
            "County Clean-[sqlproxy.0gswhr41fpjgq51exsln71gujrps].[none:County Clean:nk]-value": "county",
            "Measure Names-[sqlproxy.0gswhr41fpjgq51exsln71gujrps].[:Measure Names]-alias": "variable",
            "SUM(Population)-[sqlproxy.0tmpcbc05kyg2t1axg02e1ca3s1e].[sum:Population:qk]-alias": "population",
            "Measure Values-alias": "value",
        }
        return (
            data.loc[:, list(cols.keys())]
            .rename(columns=cols)
            # entries with min(1) as the variable are duplicated, so remove them
            .loc[lambda row: row["variable"] != "min(1)"]
            .assign(
                # calculate actual values by multiplying population * percent vaccinated
                value=lambda row: (
                    pd.to_numeric(row["value"].str.replace("%", "")) / 100
                )
                * row["population"],
                vintage=self._retrieve_vintage(),
            )
            .pipe(
                self._rename_or_add_date_and_location,
                timezone="US/Hawaii",
                location_name_column="county",
            )
            .pipe(self.extract_CMU, cmu=self.variables)
        )

    def get_filters(self):
        url = f"{self.baseurl}/views/{self.viewPath}"
        print(url)
        req = requests_retry_session()
        # info, fdat = self.getRawTbluPageData(url, tbsroot, reqParams)
        r = req.get(
            url,
            params={
                ":language": "en",
                ":display_count": "y",
                ":origin": "viz_share_link",
                ":embed": "y",
                ":showVizHome": "n",
                ":jsdebug": "y",
                ":apiID": "host4",
                "#navType": "1",
                "navSrc": "Parse",
            },
            headers={"Accept": "text/javascript"},
        )

        soup = BeautifulSoup(r.text, "html.parser")
        # return soup
        tableau_data = json.loads(
            soup.find("textarea", {"id": "tsConfigContainer"}).text
        )

        # Call the bootstrapper: grab the state data, map selection update function
        dataUrl = f'{self.baseurl}{tableau_data["vizql_root"]}/bootstrapSession/sessions/{tableau_data["sessionid"]}'
        r = requests.post(
            dataUrl,
            data={
                "sheet_id": tableau_data["sheetId"],
                "showParams": tableau_data["showParams"],
            },
        )
        # Regex the non-json output
        dat = re.search("\d+;({.*})\d+;({.*})", r.text, re.MULTILINE)
        # load info head and data group separately
        info = json.loads(dat.group(1))
        fdat = json.loads(dat.group(2))
        user_actions = info["worldUpdate"]["applicationPresModel"]["workbookPresModel"][
            "dashboardPresModel"
        ]["userActions"]
        return (info, fdat, user_actions)
