import json

from typing import Any

import pandas as pd
import us

from can_tools.scrapers import CMU, variables
from can_tools.scrapers.util import flatten_dict
from can_tools.scrapers.official.base import MicrosoftBIDashboard

"""
    Notes:
    J&J shot included in "one shot" data, but NOT in two shot data
"""


class SDVaccineCounty(MicrosoftBIDashboard):

    has_location = False
    location_type = "county"
    state_fips = int(us.states.lookup("South Dakota").fips)

    source = "https://doh.sd.gov/COVID/Dashboard.aspx"
    source_name = "South Dakota Department of Health"
    powerbi_url = "https://wabi-us-gov-iowa-api.analysis.usgovcloudapi.net"

    variables = {
        "at_least_one_dose": variables.INITIATING_VACCINATIONS_ALL,
        "total_doses": variables.TOTAL_DOSES_ADMINISTERED_ALL,
    }

    def construct_body(self, resource_key, ds_id, model_id, report_id):
        "Build body request"
        body = {}

        # Set version
        body["version"] = "1.0.0"
        body["cancelQueries"] = []
        body["modelId"] = model_id

        # used the orignal query (from the network call) because without the "WHERE" clause the # of vaccines initiated were not returned
        body["queries"] = [
            {
                "Query": {
                    "Commands": [
                        {
                            "SemanticQueryDataShapeCommand": {
                                "Binding": {
                                    "DataReduction": {
                                        "DataVolume": 3,
                                        "Primary": {"Window": {"Count": 100}},
                                        "Secondary": {"Top": {"Count": 100}},
                                    },
                                    "Primary": {
                                        "Groupings": [
                                            {"Projections": [2]},
                                            {"Projections": [1]},
                                        ]
                                    },
                                    "Secondary": {
                                        "Groupings": [
                                            {"Projections": [3, 0], "Subtotal": 2}
                                        ]
                                    },
                                    "Version": 1,
                                },
                                "Query": {
                                    "From": [
                                        {"Entity": " Measures", "Name": "m", "Type": 0},
                                        {"Entity": "County", "Name": "c", "Type": 0},
                                        {"Entity": "Vaccines", "Name": "v", "Type": 0},
                                    ],
                                    "OrderBy": [
                                        {
                                            "Direction": 2,
                                            "Expression": {
                                                "Measure": {
                                                    "Expression": {
                                                        "SourceRef": {"Source": "m"}
                                                    },
                                                    "Property": "Number of Recipients",
                                                }
                                            },
                                        }
                                    ],
                                    "Select": [
                                        {
                                            "Measure": {
                                                "Expression": {
                                                    "SourceRef": {"Source": "m"}
                                                },
                                                "Property": "Number of Recipients",
                                            },
                                            "Name": "Measures Table.Recipient Count",
                                        },
                                        {
                                            "Column": {
                                                "Expression": {
                                                    "SourceRef": {"Source": "c"}
                                                },
                                                "Property": "Doses Administered",
                                            },
                                            "Name": "County.Doses Administered",
                                        },
                                        {
                                            "Column": {
                                                "Expression": {
                                                    "SourceRef": {"Source": "c"}
                                                },
                                                "Property": "County",
                                            },
                                            "Name": "County.County",
                                        },
                                        {
                                            "Column": {
                                                "Expression": {
                                                    "SourceRef": {"Source": "v"}
                                                },
                                                "Property": "# Persons per Dose Count",
                                            },
                                            "Name": "Vaccines.# Persons per Dose Count",
                                        },
                                    ],
                                    "Version": 2,
                                    "Where": [
                                        {
                                            "Condition": {
                                                "In": {
                                                    "Expressions": [
                                                        {
                                                            "Column": {
                                                                "Expression": {
                                                                    "SourceRef": {
                                                                        "Source": "v"
                                                                    }
                                                                },
                                                                "Property": "IsMostRecentDose",
                                                            }
                                                        }
                                                    ],
                                                    "Values": [
                                                        [{"Literal": {"Value": "true"}}]
                                                    ],
                                                }
                                            }
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
                                                                                "Source": "c"
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
                                                                            "Value": "null"
                                                                        }
                                                                    }
                                                                ]
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
                                                                "Expression": {
                                                                    "SourceRef": {
                                                                        "Source": "v"
                                                                    }
                                                                },
                                                                "Property": "Exclude",
                                                            }
                                                        }
                                                    ],
                                                    "Values": [
                                                        [
                                                            {
                                                                "Literal": {
                                                                    "Value": "false"
                                                                }
                                                            }
                                                        ]
                                                    ],
                                                }
                                            }
                                        },
                                    ],
                                },
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

        # Build post body
        body = self.construct_body(resource_key, ds_id, model_id, report_id)

        res = self.sess.post(url, json=body, headers=headers)

        return res.json()

    def normalize(self, resjson):
        # Extract components we care about from json
        foo = resjson["results"][0]["result"]["data"]
        data = foo["dsr"]["DS"][0]["PH"][0]["DM0"]
        # total doses admin data are stored in different part of response
        total_doses = foo["dsr"]["DS"][0]["ValueDicts"]["D0"]

        data_rows = []
        # make the mappings manually
        col_mapping = {
            "G0": "county",
            "M_0_DM1_0_X_2_A0": "at_least_one_dose",
        }
        for i, record in enumerate(data):
            flat_record = flatten_dict(record)

            row = {}
            for k in list(col_mapping.keys()):
                flat_record_key = [frk for frk in flat_record.keys() if k in frk]

                if len(flat_record_key) > 0:
                    row[col_mapping[k]] = flat_record[flat_record_key[0]]
                    # append total doses administered data
                    row["total_doses"] = int(total_doses[i].replace(",", ""))

            data_rows.append(row)

        # Dump records into a DataFrame and transform
        df = pd.DataFrame.from_records(data_rows)
        out = self._rename_or_add_date_and_location(
            df,
            location_name_column="county",
            timezone="US/Central",
            location_names_to_drop=["Other"],
        )
        out = self._reshape_variables(out, self.variables).dropna()
        return out.replace(
            {"location_name": {"Mccook": "McCook", "Mcpherson": "McPherson"}}
        )
