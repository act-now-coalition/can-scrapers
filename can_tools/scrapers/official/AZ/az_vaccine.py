import camelot
import pandas as pd
import us

from can_tools.scrapers import variables
from can_tools.scrapers.base import CMU
from can_tools.scrapers.official.base import StateDashboard, TableauDashboard


class NoTableauMapFiltersFoundError(Exception):
    """Raised if no Tableau Map Filters are found."""


class ArizonaVaccineCounty(TableauDashboard):
    """
    Fetch county level covid data from Arizona's Tableau dashboard
    """

    baseurl = "https://tableau.azdhs.gov"
    viewPath = "VaccineDashboard/Vaccineadministrationdata"

    # Initlze
    source = "https://www.azdhs.gov/preparedness/epidemiology-disease-control/infectious-disease-epidemiology/covid-19/dashboards/index.php"
    source_name = "Arizona Department Of Health Services"
    has_location = False
    location_type = "county"
    state_fips = int(us.states.lookup("Arizona").fips)
    location_name_col = "location_name"
    timezone = "US/Mountain"

    filterFunctionName = (
        "[federated.1rsrm840sp0wgc11a5yw61x1aht3].[Calculation_624592978064752643]~s0"
    )
    counties = [
        ["APACHE", 4001],
        ["COCHISE", 4003],
        ["COCONINO", 4005],
        ["GILA", 4007],
        ["GRAHAM", 4009],
        ["GREENLEE", 4011],
        ["LA PAZ", 4012],
        ["MARICOPA", 4013],
        ["MOHAVE", 4015],
        ["NAVAJO", 4017],
        ["PIMA", 4019],
        ["PINAL", 4021],
        ["SANTA CRUZ", 4023],
        ["YAVAPAI", 4025],
        ["YUMA", 4027],
    ]

    cmus = {
        "total_vaccine_initiated": variables.INITIATING_VACCINATIONS_ALL,
        "total_vaccine_completed": variables.FULLY_VACCINATED_ALL,
        "total_doses_administered": variables.TOTAL_DOSES_ADMINISTERED_ALL,
    }

    def fetch(self):
        dfs = []
        for county_name, fips in self.counties:
            self.filterFunctionValue = county_name
            county_data = self.get_tableau_view()
            data = {
                "total_doses_administered": self._get_doses_administered(county_data),
                "total_vaccine_initiated": self._get_vaccines_initiated(county_data),
                "total_vaccine_completed": self._get_vaccines_completed(county_data),
                "location": fips,
                "location_name": county_name,
            }
            dfs.append(pd.DataFrame(data))

        # Concat the dfs
        output_df = pd.concat(dfs, axis=0, ignore_index=True)
        return output_df

    def _get_doses_administered(self, data):
        return data["Number of Doses"]["AGG(Number of Doses)-alias"]

    def _get_vaccines_completed(self, data):
        return data["Complete Vaccine Series"][
            "AGG(Total number of people (LOD))-alias"
        ]

    def _get_vaccines_initiated(self, data):
        return data[" County using admin county"]["AGG(Number of People)-alias"]
