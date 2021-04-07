import pandas as pd
import us

from can_tools.scrapers import variables
from can_tools.scrapers.official.base import (TableauDashboard, ArcGIS)

class SCVaccineCountyTaleau(TableauDashboard):
    has_location = False
    location_type = "county"
    source = "https://scdhec.gov/covid19/covid-19-vaccination-dashboard"
    source_name = "South Carolina Department of Health and Environmental Control"
    state_fips = int(us.states.lookup("South Carolina").fips)

    baseurl = "https://public.tableau.com"
    viewPath = "COVIDVaccineDashboard/RECIPIENTVIEW"

    data_tableau_table = 'County Table People Sc Residents'

    variables = {
        'Count of Doses': variables.TOTAL_DOSES_ADMINISTERED_ALL,
        'SC Residents with at least 1 Vaccine': variables.INITIATING_VACCINATIONS_ALL,

    }

    def normalize(self, data):
        df = data.rename(columns={
            'Measure Values-alias': 'value',
            "Recipient County for maps-value": 'county',
            "Recipient County for maps-alias": "county_alias",
            "Measure Names-value": "measure_value",
            "Measure Names-alias": "measure"
        })

        keep = df[['county', 'measure', 'value']]
        keep.value = pd.to_numeric(keep.value.str.replace(',', ''))

        pivoted = keep.pivot_table(index='county', values='value', columns='measure').reset_index()

        # This doesn't match up...
        # pivoted['completed'] = pivoted['Count of Doses'] - pivoted['SC Residents with at least 1 Vaccine']


        return pivoted

class SCCountyVaccineArcGIS(ArcGIS):
    ARCGIS_ID = 'XZg2efAbaieYAXmu'
    service = 'VaccineProvider_County_v2_HUB_NEW'
    def fetch(self):
        return self.get_all_jsons(self.service, 0, "2")

    def normalize(self, data):
        return data