import asyncio

import pandas as pd
import us
import os

from can_tools.scrapers import variables
from can_tools.scrapers.official.base import StateDashboard, MicrosoftBIDashboard
from can_tools.scrapers.puppet import with_page
from can_tools.scrapers.util import flatten_dict


class WashingtonVaccine(StateDashboard):
    has_location = False
    location_type = "county"
    state_fips = int(us.states.lookup("Washington").fips)
    source = "https://www.doh.wa.gov/Emergencies/COVID19/DataDashboard"
    source_name = "Washington State Department of Health"

    variables = {
        "People Initiating Vaccination": variables.INITIATING_VACCINATIONS_ALL,
        "People Fully Vaccinated": variables.FULLY_VACCINATED_ALL,
    }

    async def _get_from_browser(self):
        async with with_page(headless=True) as page:
            await page.goto(
                "https://www.doh.wa.gov/Emergencies/COVID19/DataDashboard#downloads"
            )
            sel = "#accVaccinationsTbl table"
            table_div = await page.waitForSelector(sel)
            print("found table!")
            table = await page.J(sel)
            return await page.evaluate(" x => x.outerHTML", table)

    def fetch(self):
        return asyncio.get_event_loop().run_until_complete(self._get_from_browser())

    def normalize(self, data: str) -> pd.DataFrame:
        return (
            pd.read_html(data)[0]
            .query("County != 'Total' and County != 'Unassigned'")
            .rename(columns={"County": "location_name"})
            .melt(id_vars=["location_name"], value_vars=self.variables.keys())
            .assign(
                dt=self._retrieve_dt("America/Los_Angeles"),
                vintage=self._retrieve_vintage(),
            )
            .pipe(self.extract_CMU, cmu=self.variables)
            .drop(["variable"], axis="columns")
        )


class WashingtonVaccineCountyRace(MicrosoftBIDashboard):
    has_location = False
    location_type = "county"
    state_fips = int(us.states.lookup("Washington").fips)
    source_name = "Washington State Department of Health"

    source = "https://www.doh.wa.gov/Emergencies/COVID19/DataDashboard"
    powerbi_url = "https://wabi-us-gov-virginia-api.analysis.usgovcloudapi.net"
    powerbi_dashboard_link = "https://app.powerbigov.us/view?r=eyJrIjoiZDkzMGJhYjMtZmUzMS00NDkzLWE5MTAtNGFjZjYzMmVlYzg5IiwidCI6IjExZDBlMjE3LTI2NGUtNDAwYS04YmEwLTU3ZGNjMTI3ZDcyZCJ9"

    variables = {
        "initiated": variables.INITIATING_VACCINATIONS_ALL,
        "completed": variables.FULLY_VACCINATED_ALL,
    }
    col_mapping = {
        "G0": "location_name",
        "M_1_DM3_{demo}_C_1": "initiated",
        "M_1_DM3_{demo}_C_2": "completed",
    }
    demographic_key = {
        "1": "unknown",
        "2": "white",
        "3": "asian",
        "4": "other",
        "5": "hispanic",
        "6": "black",
        "7": "ai_an",
        "8": "pacific_islander",
    }

    def get_dashboard_iframe(self):
        fumn = {"src": self.powerbi_dashboard_link}
        return fumn

    def construct_body(self, resource_key, ds_id, model_id, report_id, county):
        "Build body request"
        body = {}

        # Set version
        body["version"] = "1.0.0"
        body["cancelQueries"] = []
        body["modelId"] = model_id

        where_clause = [
            {
                "Condition": {
                    "Not": {
                        "Expression": {
                            "In": {
                                "Expressions": [
                                    {
                                        "Column": {
                                            "Expression": {
                                                "SourceRef": {"Source": "r1"}
                                            },
                                            "Property": "Race",
                                        }
                                    }
                                ],
                                "Values": [
                                    [{"Literal": {"Value": "'Total Number'"}}],
                                    [
                                        {
                                            "Literal": {
                                                "Value": "'Total with Race/Ethnicity Available'"
                                            }
                                        }
                                    ],
                                ],
                            }
                        }
                    }
                }
            },
            {
                "Condition": {
                    "In": {
                        "Expressions": [
                            {
                                "Column": {
                                    "Expression": {"SourceRef": {"Source": "_"}},
                                    "Property": "NAME",
                                }
                            }
                        ],
                        "Values": [[{"Literal": {"Value": f"'{county} County'"}}]],
                    }
                }
            },
        ]

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
                                            ("r1", "Race Table View CombinedOther", 0),
                                            ("_", "_counties_US_only", 0),
                                        ]
                                    ),
                                    "Select": self.construct_select(
                                        [
                                            # Selects
                                            ("_", "NAME", "county"),
                                            ("r1", "Race", "race"),
                                        ],
                                        [
                                            # Aggregations
                                        ],
                                        [
                                            # Measures
                                            (
                                                "r1",
                                                "VaccCountInitRaceCombinedOther2",
                                                "init",
                                            ),
                                            (
                                                "r1",
                                                "VaccCountFullyRaceCombinedOther2",
                                                "complete",
                                            ),
                                        ],
                                    ),
                                    "Where": where_clause,
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

        counties = list(
            pd.read_csv(
                os.path.dirname(__file__) + "/../../../bootstrap_data/locations.csv"
            ).query("state == @self.state_fips and name != 'Washington'")["name"]
        )

        jsons = []
        """
        make one call per county to ensure that all the data is received
        """
        for county in counties:
            # if county == 'Lewis':
            #     break
            print("making request for: ", county)
            body = self.construct_body(resource_key, ds_id, model_id, report_id, county)
            res = self.sess.post(url, json=body, headers=headers)
            jsons.append(res.json())
        return jsons

    def normalize(self, resjson: dict) -> pd.DataFrame:
        data = []
        for chunk in resjson:
            foo = chunk["results"][0]["result"]["data"]
            d = foo["dsr"]["DS"][0]["PH"][1]["DM1"]
            data.extend(d)

        # Iterate through all of the rows and store relevant data
        data_rows = []
        for record in data:
            flat_record = flatten_dict(record)

            for demo_key, demo in self.demographic_key.items():
                row = {}
                row["race"] = demo
                for k in list(self.col_mapping.keys()):
                    k_formatted = k.format(demo=demo_key)
                    flat_record_key = [
                        frk for frk in flat_record.keys() if k_formatted in frk
                    ]
                    if len(flat_record_key) > 0:
                        row[self.col_mapping[k]] = flat_record[flat_record_key[0]]
                data_rows.append(row)

        # combine into dataframe
        df = (
            pd.DataFrame.from_records(data_rows)
            .dropna()
            .assign(
                initiated=lambda x: x["initiated"].astype(str).str.replace("L", ""),
                completed=lambda x: x["completed"].astype(str).str.replace("L", ""),
                location_name=lambda x: x["location_name"].str.replace(" County", ""),
                dt=self._retrieve_dtm1d("US/Eastern"),
                vintage=self._retrieve_vintage(),
            )
        )
        out = self._reshape_variables(
            df,
            self.variables,
            id_vars=["race"],
            skip_columns=["race"],
        )

        # shift 'hispanic' entries into ethnicity column
        # mark ethnicity as unknown for unknown race columns b/c the variable is 'unknown race/ethnicity'
        hisp_rows = out["race"] == "hispanic"
        out.loc[hisp_rows, "ethnicity"] = "hispanic"
        out.loc[hisp_rows, "race"] = "all"
        unknown_rows = out["race"] == "unknown"
        out.loc[unknown_rows, "ethnicity"] = "unknown"

        return out
