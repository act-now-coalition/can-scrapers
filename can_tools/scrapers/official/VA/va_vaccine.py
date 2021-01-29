import pandas as pd
import us

from can_tools.scrapers.base import CMU
from can_tools.scrapers.official.base import TableauDashboard


class VirginiaStateVaccine(TableauDashboard):
    provider = "state"
    state_fips = int(us.states.lookup("California").fips)
    source = "https://www.vdh.virginia.gov/coronavirus/covid-19-vaccine-summary/"
    has_location = False
    baseurl = "https://vdhpublicdata.vdh.virginia.gov"
    viewPath = "VirginiaCOVID-19Dashboard-VaccineSummary/VirginiaCOVID-19VaccineSummary"
    

    def fetch(self):
        self.filterFunctionName = None
        return self.get_tableau_view()
    
    def normalize(self):
        return


class VirginiaCountyVaccine(TableauDashboard):
    provider = "county"
    state_fips = int(us.states.lookup("California").fips)
    source = "https://www.vdh.virginia.gov/coronavirus/covid-19-vaccine-summary/"
    has_location = False
