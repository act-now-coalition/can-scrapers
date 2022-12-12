from abc import ABC

import pandas as pd
import us

from can_tools.scrapers import variables
from can_tools.scrapers.official.base import MicrosoftBIDashboard
from can_tools.scrapers.util import flatten_dict


class PennsylvaniaVaccineDemographics(MicrosoftBIDashboard, ABC):
    """
    Fetch state level demographic vaccine data from Pennsylvania's PowerBI dashboard
    """

    has_location = False
    location_type = ""
    state_fips = int(us.states.lookup("Pennsylvania").fips)

    # source = "https://www.health.pa.gov/topics/disease/coronavirus/Vaccine/Pages/Vaccine.aspx"
    source = "https://www.health.pa.gov/topics/disease/coronavirus/Vaccine/Pages/Dashboard.aspx"

    source_name = "Pennsylvania Department of Health"
    powerbi_url = "https://wabi-us-gov-iowa-api.analysis.usgovcloudapi.net"

    powerbi_table: str
    powerbi_dem_column: str
    demographic: str
    value_renamer: dict
    locations_to_drop = [
        "",
        "Out-Of-State",
        "Out-Of-State*",
        "Out-of-State",
        "Out-of-State*",
    ]
    location_names_to_replace = {"Mckean": "McKean"}
    demographic_data = True

    # Reshape
    variables = {
        "initiated": variables.INITIATING_VACCINATIONS_ALL,
        "Fully Covered": variables.FULLY_VACCINATED_ALL,
    }

    def _query(self):
        return {
            "Version": 2,
            "From": self.construct_from(
                [
                    (
                        "vg",
                        self.powerbi_table,
                        0,
                    ),
                    ("v", "Vaccinations_County_ID", 0),
                ]
            ),
            "Select": self.construct_select(
                [
                    ("v", "County", "county"),
                    ("vg", "Coverage", "coverage"),
                    ("vg", self.powerbi_dem_column, self.demographic),
                ],
                [("vg", "Total Count", 0, "count")],
                [],
            ),
        }

    def construct_body(self, resource_key, ds_id, model_id, report_id):
        body = {}

        # Set version
        body["version"] = "1.0.0"
        body["cancelQueries"] = []
        body["modelId"] = model_id

        body["queries"] = [
            {
                "Query": {
                    "Commands": [
                        {
                            "SemanticQueryDataShapeCommand": {
                                "Binding": {
                                    "DataReduction": {
                                        "DataVolume": 4,
                                        "Primary": {"Window": {"Count": 5000}},
                                    },
                                    "Primary": {
                                        "Groupings": [
                                            {"Projections": [0, 1, 2, 3]},
                                        ]
                                    },
                                },
                                "Query": self._query(),
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

        url = self.powerbi_query_url()
        headers = self.construct_headers(resource_key)
        body = self.construct_body(resource_key, ds_id, model_id, report_id)
        res = self.sess.post(url, json=body, headers=headers)
        return res.json()

    def normalize(self, resjson):
        # Extract components we care about from json
        foo = resjson["results"][0]["result"]["data"]
        descriptor = foo["descriptor"]["Select"]
        data = foo["dsr"]["DS"][0]["PH"][1]["DM1"]

        # Build dict of dicts with relevant info
        col_mapping = {x["Value"]: x["Name"] for x in descriptor}
        vds = foo["dsr"]["DS"][0]["ValueDicts"]

        # Iterate through all of the rows and store relevant data
        data_rows = []
        data_labels = [x["N"] for x in data[0]["S"]]
        row_dict = {x: 0 for x in data_labels}
        for record in data:
            # We zip the backwards lists because when a value doesn't change
            # (for example if same location) then it doesn't list that value
            # in the next row...
            row = dict(zip(data_labels[::-1], record["C"][::-1]))

            row_dict.update(row)

            data_rows.append(row_dict.copy())

        # Dump records into a DataFrame
        df = pd.DataFrame.from_records(data_rows).dropna().rename(columns=col_mapping)

        # Replace indexes with values
        county_replacer = {i: vd for i, vd in enumerate(vds["D0"])}
        coverage_replacer = {i: vd for i, vd in enumerate(vds["D1"])}
        dem_replacer = {i: vd for i, vd in enumerate(vds["D2"])}
        df = (
            df.query("coverage < 2")
            .replace(
                {
                    "county": county_replacer,
                    "coverage": coverage_replacer,
                    self.demographic: dem_replacer,
                }
            )
            .pivot_table(
                index=["county", self.demographic],
                columns="coverage",
                values="count",
                aggfunc="sum",
            )
            # .fillna(0)
            .assign(initiated=lambda x: x.sum(axis=1))  # initiated = partial+full
            .reset_index()
            .rename_axis(columns=None)
        )
        out = self._rename_or_add_date_and_location(
            df,
            location_name_column="county",
            location_names_to_drop=self.locations_to_drop,
            location_names_to_replace=self.location_names_to_replace,
            timezone="US/Eastern",
        )
        out["location_name"] = out["location_name"].replace()
        out["location_type"] = "county"
        out.loc[:, "location_type"] = out.loc[:, "location_type"].where(
            out.loc[:, "location_name"] != "Pennsylvania",
            "state",
        )
        out = self._reshape_variables(
            out,
            self.variables,
            skip_columns=[self.demographic],
            id_vars=[self.demographic, "location_type"],
        )
        return out.replace({self.demographic: self.value_renamer})


class PennsylvaniaVaccineAge(PennsylvaniaVaccineDemographics):
    powerbi_table = "Vaccinations by Age_Group"
    powerbi_dem_column = "Age_Group"
    demographic = "age"
    value_renamer = {"105+": "105_plus", "05-09": "5-9"}


class PennsylvaniaVaccineEthnicity(PennsylvaniaVaccineDemographics):

    powerbi_table = "Vaccinations by Ethnicity"
    powerbi_dem_column = "Ethnicity"
    demographic = "ethnicity"
    value_renamer = {
        "Hispanic": "hispanic",
        "Not Hispanic": "non-hispanic",
        "Unknown": "unknown",
    }


class PennsylvaniaVaccineRace(PennsylvaniaVaccineDemographics):
    powerbi_table = "Vaccinations by Race"
    powerbi_dem_column = "Race"
    demographic = "race"
    value_renamer = {
        "White": "white",
        "African American": "black",
        "Asian": "asian",
        "Pacific Islander": "pacific_islander",
        "Native American": "ai_an",
        "Multiple/Other": "multiple_other",
        "Unknown": "unknown",
    }


class PennsylvaniaVaccineSex(PennsylvaniaVaccineDemographics):
    powerbi_table = "Vaccinations by Gender"
    powerbi_dem_column = "Gender"
    demographic = "sex"
    value_renamer = {"Male": "male", "Female": "female", "Unknown": "unknown"}
