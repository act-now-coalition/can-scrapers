import pandas as pd
import us

from can_tools.scrapers.official.base import TableauDashboard
from can_tools.scrapers import variables
class IdahoCountyVaccine(TableauDashboard):
    has_location = False
    source = "https://coronavirus.idaho.gov/"
    source_name = "Idaho Official Government Website"
    data_tableau_table = 'Vax Rate / County Chart
    baseurl = "https://public.tableau.com"
    viewPath = "COVID-19VaccineDataDashboard/VaccineUptake"

    state_fips = int(us.states.lookup("Idaho").fips)

    variables = {
        '1': variables.INITIATING_VACCINATIONS_ALL,
        '2': variables.FULLY_VACCINATED_ALL
    }

    def fetch(self):

        return self.get_tableau_view()
    
    def normalize(self, data):
        df = data['Vax Rate / County Chart']
        df.columns = [
            'doses',
            'vax_rate_value',
            'vax_rate_alias',
            "district_value",
            "district_alias",
            "county",
            "phd_county_alias",
            "dose_number",
            "dose_number_alias",
            "district_alias"
        ]
        keep = df[['county', 'doses', 'dose_number']]
        out =  keep.pivot(index='county', columns='dose_number', values='doses').reset_index().rename_axis(None, axis=1)
        # It seems like people who are fully vaccinated are no longer counted in the
        # "people who have received one dose" category. Summing these two together to 
        # match our definition
        out['1'] = out['1'] + out['2']

        out = self._rename_or_add_date_and_location(
            out,
            location_name_column="county",
            timezone="US/Mountain"
        )
        out = self._reshape_variables(out, self.variables)
        
        return out
