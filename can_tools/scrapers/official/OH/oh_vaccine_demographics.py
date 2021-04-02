import pandas as pd
import us

from can_tools.scrapers.base import ScraperVariable
from can_tools.scrapers.official.base import TableauDashboard


class OHVaccineDemographics(TableauDashboard):
    has_location = False
    source = "https://coronavirus.ohio.gov/wps/portal/gov/covid-19/dashboards/covid-19-vaccine/covid-19-vaccination-dashboard"
    source_name = "Ohio State Department of Health"
    state_fips = int(us.states.lookup("Ohio").fips)
    location_type = "county"
    baseurl = "https://public.tableau.com/"
    viewPath = "VaccineAdministrationMetricsDashboard/PublicCountyDash"

    data_tableau_table = ""
    location_name_col = ""
    timezone = "US/Eastern"

    # map wide form column names into ScraperVariables
    cmus = {}
