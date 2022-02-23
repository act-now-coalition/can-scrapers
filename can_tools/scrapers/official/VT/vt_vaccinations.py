from typing import Dict
import pandas as pd
import requests
import us

from can_tools.scrapers import variables
from functools import reduce
from can_tools.scrapers.official.base import MicrosoftBIDashboard
from bs4 import BeautifulSoup


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
        "total_vaccine_additional_dose": variables.PEOPLE_VACCINATED_ADDITIONAL_DOSE,
    }

    def get_dashboard_iframe(self):
        """scrapes the dashboard source page for the link (iframe) to the stand-alone dashboard.

        The iframe from the source page directed to an arcgis url, which does not comply with this
        scraper class' setup/methods, so this custom method is used instead to grab the
        standard 'powerbigov' url.
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

    def construct_body(self, ds_id, model_id, report_id, dose_type):
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
                                            ("c", "Dimension County", 0),
                                            ("b", "Denominator Age/Sex", 0),
                                            ("s1", "Select vaccine status", 0),
                                            ("d1", "Dimension Age - Age", 0),
                                            ("m", "_CountyMeasures", 0),
                                        ]
                                    ),
                                    "Select": self.construct_select(
                                        [
                                            # Selects
                                            ("c", "County", "location_name")
                                        ],
                                        [("b", "Count", 0, "count")],
                                        [
                                            # Measures
                                            ("m", "Map % vaccinated", "init"),
                                            # ("i", "People completed 1", "completed"),
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
                                                                        "Source": "s1"
                                                                    }
                                                                },
                                                                "Property": "Select vax status",
                                                            }
                                                        }
                                                    ],
                                                    "Values": [
                                                        [
                                                            {
                                                                # 'Completed'
                                                                "Literal": {
                                                                    "Value": f"'{dose_type}'"
                                                                }
                                                            }
                                                        ]
                                                    ],
                                                }
                                            },
                                        },
                                        {
                                            "Condition": {
                                                "Not": {
                                                    "Expression": {
                                                        "In": {
                                                            "Expressions": [
                                                                {
                                                                    "Column": {
                                                                        "Expression": {
                                                                            "SourceRef": {
                                                                                "Source": "d1"
                                                                            }
                                                                        },
                                                                        "Property": "Age eligibility 5+",
                                                                    }
                                                                }
                                                            ],
                                                            "Values": [
                                                                [
                                                                    {
                                                                        "Literal": {
                                                                            "Value": "'Under 5'"
                                                                        }
                                                                    }
                                                                ]
                                                            ],
                                                        }
                                                    }
                                                }
                                            }
                                        },
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

        jsons = {}
        # Build post body
        for dose in ["1 or more", "Completed", "Additional dose"]:
            body = self.construct_body(ds_id, model_id, report_id, dose_type=dose)
            res = self.sess.post(url, json=body, headers=headers).json()
            jsons[dose] = res

        return jsons

    def normalize(self, resjson: dict) -> pd.DataFrame:
        dose_dfs = [
            self._extract_dose_data(dose, data) for dose, data in resjson.items()
        ]

        # merge dose dataframes and multiply percentage * population to find cumulative values
        df = reduce(
            lambda left, right: pd.merge(
                left, right, on=["location_name", "pop_5_plus"]
            ),
            dose_dfs,
        )
        df["total_vaccine_initiated"] = df["1 or more"] * df["pop_5_plus"]
        df["total_vaccine_completed"] = df["Completed"] * df["pop_5_plus"]
        df["total_vaccine_additional_dose"] = df["Additional dose"] * df["pop_5_plus"]

        out = self._reshape_variables(df, self.variables)
        out["dt"] = self._retrieve_dt("US/Eastern")
        return out

    @staticmethod
    def _extract_dose_data(dose_title: str, resjson: Dict):
        """extract all data records from the JSON fetch response"""
        foo = resjson["results"][0]["result"]["data"]
        data = foo["dsr"]["DS"][0]["PH"][1]["DM1"]

        records = [record["C"] for record in data]
        df = pd.DataFrame(records, columns=["location_name", "pop_5_plus", dose_title])
        df[dose_title] = pd.to_numeric(df[dose_title].str.replace("D", ""))
        return df
