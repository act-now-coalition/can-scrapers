import pandas as pd
import us

from can_tools.scrapers.base import CMU
from can_tools.scrapers.official.base import TableauDashboard


class NewYorkVaccineCounty(TableauDashboard):
    has_location = False
    source = "https://covid19vaccine.health.ny.gov/covid-19-vaccine-tracker"
    source_name = "New York State Department of Health"
    state_fips = int(us.states.lookup("New York").fips)
    location_type = "county"
    baseurl = "https://covid19tracker.health.ny.gov"
    viewPath = "Vaccine_County_Public/NYSCountyVaccinations"

    data_tableau_table = "Vaccinated by County"
    location_name_col = "County-alias"
    timezone = "US/Eastern"

    cmus = {
        "SUM(First Dose)-alias": CMU(
            category="total_vaccine_initiated",
            measurement="cumulative",
            unit="people",
        ),
        "SUM(Second Dose)-alias": CMU(
            category="total_vaccine_completed",
            measurement="cumulative",
            unit="people",
        ),
    }
