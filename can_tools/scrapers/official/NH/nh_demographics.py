from tableauscraper import TableauScraper
import us
import pandas as pd
from can_tools.scrapers import variables
import multiprocessing
from functools import partial
import pkg_resources
import logging


from can_tools.scrapers.official.base import StateDashboard

NUM_PROCESSES = 12


class NHVaccineRace(StateDashboard):
    has_location = False
    source = "https://www.covid19.nh.gov/dashboard/vaccination"
    source_name = "New Hampshire DHHS"
    location_type = "county"

    state_fips = us.states.lookup("New Hampshire").fips

    variables = {
        "Residents with At Least 1 Dose:": variables.INITIATING_VACCINATIONS_ALL,
        "Residents Fully Vaccinated:": variables.FULLY_VACCINATED_ALL,
    }

    # hard coding to make parallel fetching easier
    counties = [
        "Rest of Hillsborough",
        "Sullivan",
        "Strafford",
        "Rockingham",
        "Nashua",
        "Merrimack",
        "Manchester",
        "Grafton",
        "Coos",
        "Cheshire",
        "Carroll",
        "Belknap",
    ]

    def fetch(self):

        ts_version = pkg_resources.get_distribution("tableauscraper").version
        _logger = logging.getLogger(__name__)
        _logger.warning(f"tableauscraper version {ts_version}")

        # fetch NUM_PROCESSES counties at a time
        pool = multiprocessing.Pool(processes=NUM_PROCESSES)
        data = []

        # Setting multiple parameter values on the same parameter type
        # on a single TableauScraper instance removes the
        # other selections (in this case, the county selection) from the instance, and there doesn't seem
        # to be a good way to fix/circumvent this. So, we use two TableauScraper
        # instances to fetch each respective dose type.
        # This is an issue we've had in the past with the OH and WI demographic scrapers.
        for variable in [
            "initiated",
            "completed",
        ]:
            func = partial(self._get_county, variable)
            data.extend(pool.map(func, self.counties))
        return pd.concat(data)

    @staticmethod
    def _get_county(variable: str, county: str):
        engine = TableauScraper()
        engine.loads(
            url="https://www.nh.gov/t/DHHS/views/VaccineOperationalDashboard/VaccineDashboard"
        )

        engine.getWorkbook().setParameter("ShowBy", "Race/Ethnicity")
        # By default the parameter is set to "Total Individuals Completed Vaccine"
        # and when trying to set the parameter to the default state we get a warning.
        # On the Prefect VM, this warning causes the scraper to fail.
        # To circumvent this, we set the parameter if we are pulling in 1+ dose data.
        if variable == "initiated":
            engine.getWorkbook().setParameter(
                "Metric", "Total Individuals with at least 1 Dose"
            )

        raw_data = []
        worksheet = engine.getWorksheet("Count and Prop: Map (no R/E with town&rphn)")
        workbook = worksheet.select("CMN + Town + RPHN", county)
        raw_data.append(
            workbook.getWorksheet("Count: Bar Chart").data.assign(location_name=county)
        )

        data = pd.concat(raw_data)
        return data

    def normalize(self, data) -> pd.DataFrame:
        demo_replace = {
            "Hispanic/Latino": "hispanic",
            "Black or African American": "black",
            "Other Races": "other",
        }

        data = (
            data.rename(columns={"AGG(Metric Value Nh Residents)-alias": "value"})
            .assign(
                dt=self._retrieve_dt("US/Eastern"),
                vintage=self._retrieve_vintage(),
                race=lambda row: row["Show by Filter-value"]
                .replace(demo_replace)
                .str.lower(),
            )
            .pipe(
                self.extract_CMU,
                cmu=self.variables,
                var_name="ATTR(Bar Chart Tooltip NH Res)-alias",
                skip_columns=["race"],
            )
        )

        # combine cities of manchester and nashua into hillsborough county
        cols_to_keep = [
            "vintage",
            "dt",
            "location_name",
            "category",
            "measurement",
            "unit",
            "age",
            "race",
            "ethnicity",
            "sex",
            "value",
        ]
        data = data.loc[:, cols_to_keep]
        data["location_name"] = data["location_name"].replace(
            {
                "Rest of Hillsborough": "Hillsborough",
                "Nashua": "Hillsborough",
                "Manchester": "Hillsborough",
            }
        )
        group = [col for col in cols_to_keep if col != "value"]
        data = data.groupby(group).sum().reset_index()
        # shift hispanic values to ethnicity column
        data.loc[data["race"] == "hispanic", "ethnicity"] = "hispanic"
        data.loc[data["ethnicity"] == "hispanic", "race"] = "all"
        return data
