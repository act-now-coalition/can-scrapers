from tableauscraper import TableauScraper
import us
import pandas as pd
from can_tools.scrapers import variables
import multiprocessing

from can_tools.scrapers.official.base import StateDashboard

NUM_PROCESSES = 8


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
        "Sullivan",
        "Strafford",
        "Rockingham",
        "Rest of Hillsborough",
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
        # fetch NUM_PROCESSES counties at a time
        pool = multiprocessing.Pool(processes=NUM_PROCESSES)
        data = pool.map(self._get_county, self.counties[:8])
        return pd.concat(data)

    @staticmethod
    def _get_county(county: str):
        engine = TableauScraper()
        engine.loads(
            url="https://www.nh.gov/t/DHHS/views/VaccineOperationalDashboard/VaccineDashboard"
        )

        engine.getWorkbook().setParameter("ShowBy", "Race/Ethnicity")

        raw_data = []
        for variable in [
            "Total Individuals Fully Vaccinated",
            "Total Individuals with at least 1 Dose",
        ]:
            engine.getWorkbook().setParameter("Metric", variable)
            worksheet = engine.getWorksheet(
                "Count and Prop: Map (no R/E with town&rphn)"
            )

            workbook = worksheet.select("CMN + Town + RPHN", county)
            raw_data.append(
                workbook.getWorksheet("Count: Bar Chart").data.assign(
                    location_name=county
                )
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
        return data.groupby(group).sum().reset_index()
