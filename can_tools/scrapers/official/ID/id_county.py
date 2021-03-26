import pandas as pd
import us

from can_tools.scrapers.official.base import TableauDashboard

class IdahoCountyVaccine(TableauDashboard):
    has_location = False
    source = "https://coronavirus.idaho.gov/"
    source_name = "Idaho Official Government Website"

    baseurl = "https://public.tableau.com"
    viewPath = "COVID-19VaccineDataDashboard/VaccineUptake"

    state_fips = int(us.states.lookup("Idaho").fips)

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
        out =  keep.pivot(index='county', columns='dose_number', values='doses').reset_index()
        out = out.rename(columns={
            "1": "total_vaccine_initiated",
            "2": "total_vaccine_completed"
        })
        return out