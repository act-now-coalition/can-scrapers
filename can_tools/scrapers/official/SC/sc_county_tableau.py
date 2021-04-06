import pandas as pd
import us

from can_tools.scrapers.official.base import (TableauDashboard)

class SCVaccineCountyTaleau(TableauDashboard):
    has_location = False
    location_type = "county"
    source = "https://scdhec.gov/covid19/covid-19-vaccination-dashboard"
    source_name = "South Carolina Department of Health and Environmental Control"
    state_fips = int(us.states.lookup("South Carolina").fips)

    baseurl = "https://public.tableau.com"
    viewPath = "COVIDVaccineDashboard/RECIPIENTVIEW"

    data_tableau_table = 'County Table People Sc Residents'

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

        pivoted['completed'] = pivoted['Count of Doses'] - pivoted['SC Residents with at least 1 Vaccine']


        return keep