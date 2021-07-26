import pandas as pd
import requests
import us

from can_tools.scrapers import variables
from can_tools.scrapers.official.base import MicrosoftBIDashboard
from can_tools.scrapers.util import flatten_dict
from bs4 import BeautifulSoup
import os


pd.options.mode.chained_assignment = None  # Avoid unnecessary SettingWithCopy warning


class VermontCountyVaccine(MicrosoftBIDashboard):
    has_location = False
    location_type = "county"
    state_fips = int(us.states.lookup("Vermont").fips)
    source = "https://www.healthvermont.gov/covid-19/vaccine/covid-19-vaccine-dashboard"
    source_name = "Vermont Department of Health"
    powerbi_url = "https://wabi-us-gov-virginia-api.analysis.usgovcloudapi.net"

    variables = {
        "total_vaccine_initiated": variables.INITIATING_VACCINATIONS_ALL,
        "total_vaccine_completed": variables.FULLY_VACCINATED_ALL,
    }

    def get_counties(self):
        path = "/../../../bootstrap_data/locations.csv"
        return list(
            pd.read_csv(os.path.dirname(__file__) + path).query(
                f"state == {self.state_fips} and location != {self.state_fips}"
            )["name"]
        )

    def get_dashboard_iframe(self):
        """scrapes the dashboard source page for the link (iframe) to the stand-alone dashboard.

        The iframe from the source page directed to an arcgis url, which does not comply with this
        scraper class' setup/methods, so this custom method is used.
        """

        res = requests.get(self.source)
        page = BeautifulSoup(res.content, "lxml")
        urls = page.find_all("a")
        iframe = [
            url["href"]
            for url in urls
            if url.has_attr("href") and "app.powerbigov.us" in url["href"]
        ][0]
        return {"src": iframe}

    def construct_body(self, resource_key, ds_id, model_id, report_id, county):
        "Build body request"
        body = {}
        body["version"] = "1.0.0"
        body["cancelQueries"] = []
        body["modelId"] = model_id

        body["queries"] = [
            {
                "Query": {
                    "Commands": [
                        {
                            "SemanticQueryDataShapeCommand": {
                                "Query": {
                                    "Version": 2,
                                    "From": self.construct_from(
                                        [
                                            # From
                                            ("d1", "Dimension County", 0),
                                            ("i", "Immunization Summary - finished", 0),
                                            ("_1", "_CVMeasures", 0),
                                        ]
                                    ),
                                    "Select": self.construct_select(
                                        [
                                            # Selects
                                            ("d1", "County", "location_name")
                                        ],
                                        [],
                                        [
                                            # Measures
                                            ("_1", "People vaccinated", "init"),
                                            ("i", "People completed 1", "completed"),
                                        ],
                                    ),
                                    "Where": [
                                        {
                                            "Condition": {
                                                "In": {
                                                    "Expressions": [
                                                        {
                                                            "Column": {
                                                                "Expression": {
                                                                    "SourceRef": {
                                                                        "Source": "d1"
                                                                    }
                                                                },
                                                                "Property": "County",
                                                            }
                                                        }
                                                    ],
                                                    "Values": [
                                                        [
                                                            {
                                                                "Literal": {
                                                                    "Value": f"'{county}'"
                                                                }
                                                            }
                                                        ]
                                                    ],
                                                }
                                            }
                                        }
                                    ],
                                }
                            }
                        }
                    ]
                },
                "QueryId": "",
                "ApplicationContext": self.construct_application_context(
                    ds_id, report_id
                ),
            }
        ]

        return body

    def fetch(self):

        # Get general information
        self._setup_sess()
        dashboard_frame = self.get_dashboard_iframe()
        resource_key = self.get_resource_key(dashboard_frame)
        ds_id, model_id, report_id = self.get_model_data(resource_key)

        # Get the post url
        url = self.powerbi_query_url()

        # Build post headers
        headers = self.construct_headers(resource_key)

        jsons = []
        # Build post body
        for county in self.get_counties():
            body = self.construct_body(resource_key, ds_id, model_id, report_id, county)
            res = self.sess.post(url, json=body, headers=headers)
            jsons.append(res.json())

        return jsons

    def normalize(self, resjson: dict) -> pd.DataFrame:

        # combine all list entries into one dict s.t they can all be parsed at once
        data = []
        for chunk in resjson:
            foo = chunk["results"][0]["result"]["data"]
            d = foo["dsr"]["DS"][0]["PH"][1]["DM1"]
            data.extend(d)

        # Build dict of dicts with relevant info
        col_mapping = {
            "C_0": "location_name",
            "C_1": "total_vaccine_initiated",
            "C_2": "total_vaccine_completed",
        }

        # Iterate through all of the rows and store relevant data
        data_rows = []
        for record in data:
            flat_record = flatten_dict(record)
            row = {}
            for k in list(col_mapping.keys()):
                flat_record_key = [frk for frk in flat_record.keys() if k in frk]

                if len(flat_record_key) > 0:
                    row[col_mapping[k]] = flat_record[flat_record_key[0]]

            data_rows.append(row)
        df = pd.DataFrame.from_records(data_rows).reset_index()

        out = self._reshape_variables(df, self.variables)
        out["dt"] = self._retrieve_dt("US/Eastern")
        return out
