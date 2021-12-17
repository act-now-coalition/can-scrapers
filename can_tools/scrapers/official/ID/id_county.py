import us
import pandas as pd
from can_tools.scrapers import variables
from can_tools.scrapers.official.base import TableauDashboard


class IdahoCountyVaccine(TableauDashboard):
    has_location = False
    location_type = "county"
    source = "https://coronavirus.idaho.gov/"
    source_name = "Idaho Official Government Website"
    data_tableau_table = "County Map"
    baseurl = "https://public.tableau.com"
    viewPath = "COVID-19VaccineDataDashboard/VaccineUptakeIdahoIIS"
    filterFunctionName = "[Parameters].[Map (copy)]"  # set to county level
    filterFunctionValue = "County"
    state_fips = int(us.states.lookup("Idaho").fips)

    variables = {
        "initiated": variables.INITIATING_VACCINATIONS_ALL,
        "SUM(Fully Vaccinated)-alias": variables.FULLY_VACCINATED_ALL,
    }

    def normalize(self, data):

        # It seems like people who are fully vaccinated are no longer counted in the
        # "people who have received one dose" category. Summing these two together to
        # match our definition
        data["initiated"] = data["SUM(Series in progress)-alias"] + data["SUM(Fully Vaccinated)-alias"]
        data = data[["initiated", "SUM(Fully Vaccinated)-alias", "County1-value"]]

        out = self._rename_or_add_date_and_location(
            data, location_name_column="County1-value", timezone="US/Mountain"
        )
        out = self._reshape_variables(out, self.variables)

        return out
