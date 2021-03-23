from can_tools.scrapers.variables import (
    FULLY_VACCINATED_ALL,
    INITIATING_VACCINATIONS_ALL,
)
import us

from can_tools.scrapers.official.base import TableauDashboard


class OregonVaccineCounty(TableauDashboard):
    has_location = False
    source = "https://covidvaccine.oregon.gov/"
    source_name = "Oregon Health Authority"
    state_fips = int(us.states.lookup("Oregon").fips)
    location_type = "county"

    baseurl = "https://public.tableau.com/"
    viewPath = "OregonCOVID-19VaccinationTrends/OregonCountyVaccinationTrends"

    cmus = {
        "SUM(Metric - Total People)-alias": INITIATING_VACCINATIONS_ALL,
        "SUM(Metric - Fully Vaccinated)-alias": FULLY_VACCINATED_ALL,
    }

    data_tableau_table = "County Map Per Capita new"
    location_name_col = "Recip Address County-alias"
    timezone = "US/Pacific"
