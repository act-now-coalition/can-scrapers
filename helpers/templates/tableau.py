import pandas as pd
import us

from can_tools.scrapers.base import ScraperVariable
from can_tools.scrapers.official.base import TableauDashboard


class {{ scraper.name }}(TableauDashboard):
    has_location = False
    source = "{{ scraper.source }}"
    source_name = "{{ scraper.source_name }}"
    state_fips = int(us.states.lookup("{{ scraper.state_name }}").fips)
    location_type = "county"
    baseurl = "{{ scraper.baseurl }}"
    viewPath = "{{ scraper.viewPath }}"

    data_tableau_table = "{{ scraper.data_tableau_table }}"
    location_name_col = "{{ scraper.location_name_col }}"
    timezone = "{{ scraper.timezone }}"

    # map wide form column names into ScraperVariables
    cmus = {
    }
