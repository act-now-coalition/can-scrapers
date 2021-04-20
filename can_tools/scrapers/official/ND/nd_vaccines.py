from typing import Any

import pandas as pd
import us

from can_tools.scrapers import CMU, variables
from can_tools.scrapers.util import flatten_dict
from can_tools.scrapers.official.base import MicrosoftBIDashboard

from can_tools.scrapers.official.ND.common import nd_populations


class NDVaccineCounty(MicrosoftBIDashboard):

    has_location = False
    location_type = "county"
    state_fips = int(us.states.lookup("North Dakota").fips)

    source = "https://www.health.nd.gov/covid19vaccine/dashboard"
    source_name = "North Dakota Department of Health"
    powerbi_url = "https://wabi-us-gov-iowa-api.analysis.usgovcloudapi.net"

    variables = {
        "at_least_one_dose": variables.INITIATING_VACCINATIONS_ALL,
        "fully_vaccinated": variables.FULLY_VACCINATED_ALL,
    }

    def _map_data(self, data, col_mapping, extra_param: str):
        data_rows = []

        for i, record in enumerate(data):
            flat_record = flatten_dict(record)

            # inconsistent parameters in "C" array for first county
            # number of doses is in extra param, "C_2"
            if flat_record["C_1"] == extra_param:
                flat_record["C_1"] = flat_record["C_2"]

            row = {}
            for k in list(col_mapping.keys()):
                flat_record_key = [frk for frk in flat_record.keys() if k in frk]

                if len(flat_record_key) > 0:
                    row[col_mapping[k]] = flat_record[flat_record_key[0]]

            data_rows.append(row)

        return data_rows

    def construct_body(self, resource_key, ds_id, model_id, report_id):
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
                                    "From": [
                                        {"Name": "c", "Entity": "County", "Type": 0},
                                        {
                                            "Name": "p",
                                            "Entity": "Public Metrics",
                                            "Type": 0,
                                        },
                                        {
                                            "Name": "a",
                                            "Entity": "Age Bracket",
                                            "Type": 0,
                                        },
                                        {
                                            "Name": "s",
                                            "Entity": "slicer_DoseSelector",
                                            "Type": 0,
                                        },
                                    ],
                                    "Select": [
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
                                            "Measure": {
                                                "Expression": {
                                                    "SourceRef": {"Source": "p"}
                                                },
                                                "Property": "Title - X Dose Coverage Rate Switch",
                                            },
                                            "Name": "Age Bracket.Title - X Dose Coverage Rate Switch",
                                        },
                                        {
                                            "Measure": {
                                                "Expression": {
                                                    "SourceRef": {"Source": "p"}
                                                },
                                                "Property": "Public X Dose Coverage Rate Switch",
                                            },
                                            "Name": "Public Metrics.Public X Dose Coverage Rate Switch",
                                        },
                                        {
                                            "Measure": {
                                                "Expression": {
                                                    "SourceRef": {"Source": "p"}
                                                },
                                                "Property": "Title - Coverage Rate County Map",
                                            },
                                            "Name": "Public Metrics.Title - Coverage Rate County Map",
                                        },
                                    ],
                                    "Where": [
                                        {
                                            "Condition": {
                                                "In": {
                                                    "Expressions": [
                                                        {
                                                            "Column": {
                                                                "Expression": {
                                                                    "SourceRef": {
                                                                        "Source": "c"
                                                                    }
                                                                },
                                                                "Property": "State",
                                                            }
                                                        }
                                                    ],
                                                    "Values": [
                                                        [
                                                            {
                                                                "Literal": {
                                                                    "Value": "'North Dakota'"
                                                                }
                                                            }
                                                        ]
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
                                                                                "Source": "a"
                                                                            }
                                                                        },
                                                                        "Property": "Bracket",
                                                                    }
                                                                }
                                                            ],
                                                            "Values": [
                                                                [
                                                                    {
                                                                        "Literal": {
                                                                            "Value": "'<18 years'"
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
                                                                        "Source": "s"
                                                                    }
                                                                },
                                                                "Property": "Column1",
                                                            }
                                                        }
                                                    ],
                                                    "Values": [
                                                        [
                                                            {
                                                                "Literal": {
                                                                    "Value": "'1 Dose Coverage Rate'"
                                                                }
                                                            }
                                                        ]
                                                    ],
                                                }
                                            }
                                        },
                                    ],
                                    "OrderBy": [
                                        {
                                            "Direction": 2,
                                            "Expression": {
                                                "Measure": {
                                                    "Expression": {
                                                        "SourceRef": {"Source": "p"}
                                                    },
                                                    "Property": "Public X Dose Coverage Rate Switch",
                                                }
                                            },
                                        }
                                    ],
                                },
                                "Binding": {
                                    "Primary": {
                                        "Groupings": [{"Projections": [0, 1, 2]}]
                                    },
                                    "Projections": [3],
                                    "DataReduction": {
                                        "DataVolume": 4,
                                        "Primary": {"Top": {}},
                                    },
                                    "Aggregates": [
                                        {
                                            "Select": 2,
                                            "Aggregations": [{"Min": {}}, {"Max": {}}],
                                        }
                                    ],
                                    "SuppressedJoinPredicates": [1],
                                    "Version": 1,
                                },
                            }
                        }
                    ]
                },
                "QueryId": "",
                "ApplicationContext": self.construct_application_context(
                    ds_id, report_id
                ),
            },
            {
                "Query": {
                    "Commands": [
                        {
                            "SemanticQueryDataShapeCommand": {
                                "Query": {
                                    "Version": 2,
                                    "From": [
                                        {
                                            "Name": "c",
                                            "Entity": "County",
                                            "Type": 0,
                                        },
                                        {
                                            "Name": "p",
                                            "Entity": "Public Metrics",
                                            "Type": 0,
                                        },
                                        {
                                            "Name": "a",
                                            "Entity": "Age Bracket",
                                            "Type": 0,
                                        },
                                        {
                                            "Name": "s",
                                            "Entity": "slicer_DoseSelector",
                                            "Type": 0,
                                        },
                                    ],
                                    "Select": [
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
                                            "Measure": {
                                                "Expression": {
                                                    "SourceRef": {"Source": "p"}
                                                },
                                                "Property": "Title - X Dose Coverage Rate Switch",
                                            },
                                            "Name": "Age Bracket.Title - X Dose Coverage Rate Switch",
                                        },
                                        {
                                            "Measure": {
                                                "Expression": {
                                                    "SourceRef": {"Source": "p"}
                                                },
                                                "Property": "Public X Dose Coverage Rate Switch",
                                            },
                                            "Name": "Public Metrics.Public X Dose Coverage Rate Switch",
                                        },
                                        {
                                            "Measure": {
                                                "Expression": {
                                                    "SourceRef": {"Source": "p"}
                                                },
                                                "Property": "Title - Coverage Rate County Map",
                                            },
                                            "Name": "Public Metrics.Title - Coverage Rate County Map",
                                        },
                                    ],
                                    "Where": [
                                        {
                                            "Condition": {
                                                "In": {
                                                    "Expressions": [
                                                        {
                                                            "Column": {
                                                                "Expression": {
                                                                    "SourceRef": {
                                                                        "Source": "c"
                                                                    }
                                                                },
                                                                "Property": "State",
                                                            }
                                                        }
                                                    ],
                                                    "Values": [
                                                        [
                                                            {
                                                                "Literal": {
                                                                    "Value": "'North Dakota'"
                                                                }
                                                            }
                                                        ]
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
                                                                                "Source": "a"
                                                                            }
                                                                        },
                                                                        "Property": "Bracket",
                                                                    }
                                                                }
                                                            ],
                                                            "Values": [
                                                                [
                                                                    {
                                                                        "Literal": {
                                                                            "Value": "'<18 years'"
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
                                                                        "Source": "s"
                                                                    }
                                                                },
                                                                "Property": "Column1",
                                                            }
                                                        }
                                                    ],
                                                    "Values": [
                                                        [
                                                            {
                                                                "Literal": {
                                                                    "Value": "'Up-to-Date Coverage Rate'"
                                                                }
                                                            }
                                                        ]
                                                    ],
                                                }
                                            }
                                        },
                                    ],
                                    "OrderBy": [
                                        {
                                            "Direction": 2,
                                            "Expression": {
                                                "Measure": {
                                                    "Expression": {
                                                        "SourceRef": {"Source": "p"}
                                                    },
                                                    "Property": "Public X Dose Coverage Rate Switch",
                                                }
                                            },
                                        }
                                    ],
                                },
                                "Binding": {
                                    "Primary": {
                                        "Groupings": [{"Projections": [0, 1, 2]}]
                                    },
                                    "Projections": [3],
                                    "DataReduction": {
                                        "DataVolume": 4,
                                        "Primary": {"Top": {}},
                                    },
                                    "Aggregates": [
                                        {
                                            "Select": 2,
                                            "Aggregations": [
                                                {"Min": {}},
                                                {"Max": {}},
                                            ],
                                        }
                                    ],
                                    "SuppressedJoinPredicates": [1],
                                    "Version": 1,
                                },
                            }
                        }
                    ]
                },
                "QueryId": "",
                "ApplicationContext": self.construct_application_context(
                    ds_id, report_id
                ),
            },
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

    def normalize(self, json):
        # Extract components we care about from json
        first_dose_data = json["results"][0]["result"]["data"]["dsr"]["DS"][0]["PH"][0][
            "DM0"
        ]
        fully_vaccinated_data = json["results"][1]["result"]["data"]["dsr"]["DS"][0][
            "PH"
        ][0]["DM0"]

        # first dose data mapping
        first_dose_col_mapping = {
            "C_0": "county",
            "C_1": "at_least_one_dose",
        }
        first_dose_data_rows = self._map_data(
            first_dose_data, first_dose_col_mapping, "1 Dose"
        )

        # fully vaccinated data mapping
        fully_vaccinated_col_mapping = {
            "C_0": "county",
            "C_1": "fully_vaccinated",
        }
        fully_vaccinated_data_rows = self._map_data(
            fully_vaccinated_data, fully_vaccinated_col_mapping, "Up-to-Date"
        )

        # combine first dose rows with fully vaccinated rows
        data_rows = first_dose_data_rows + fully_vaccinated_data_rows

        # multiply percentages per county by populations to get actual number of people
        data_rows_with_population = []
        for i, row in enumerate(data_rows):
            county_name = row["county"]

            # differentiate dose type from `at_least_one_dose` and `fully_vaccinated`
            keys = list(row.keys())
            dose_type = keys[1]
            dose_percentage = float(row[dose_type])

            if county_name in nd_populations:
                doses = int(dose_percentage * nd_populations[county_name])

            row_with_population = {
                "county": county_name,
                dose_type: doses,
            }

            data_rows_with_population.append(row_with_population)

        # Dump records into a DataFrame and transform
        df = pd.DataFrame.from_records(data_rows_with_population)
        out = self._rename_or_add_date_and_location(
            df,
            location_name_column="county",
            timezone="US/Central",
            location_names_to_drop=["ND-COUNTY UNKNOWN"],
        )
        out = self._reshape_variables(out, self.variables).dropna()
        out = out.replace(
            {
                "location_name": {
                    "Lamoure": "LaMoure",
                    "Mchenry": "McHenry",
                    "Mcintosh": "McIntosh",
                    "Mckenzie": "McKenzie",
                    "Mclean": "McLean",
                }
            }
        )

        return out
