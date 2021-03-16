import pandas as pd
import us

from can_tools.scrapers.base import CMU
from can_tools.scrapers.official.base import TableauDashboard

from can_tools.scrapers.util import requests_retry_session

class HawaiiVaccineCounty(TableauMapClick):
    has_location = False
    source = "https://health.hawaii.gov/coronavirusdisease2019/what-you-should-know/current-situation-in-hawaii/#vaccine"
    source_name = (
        "State of Hawai'i - Department of Health"
        "Disease Outbreak Control Division | COVID-19"
    )
    location_type = "county"
    source_url = (
        "https://public.tableau.com/views/"
        "HawaiiCOVID-19-VaccinationDashboard/VACCINESBYCOUNTY"
        "?%3Aembed=y&%3AshowVizHome=no&%3Adisplay_count=y"
        "&%3Adisplay_static_image=y&%3AbootstrapWhenNotified=true"
        "&%3Alanguage=en&:embed=y&:showVizHome=n&:apiID=host0#navType=0&navSrc=Parse"
    )
    # https://public.tableau.com/shared/H33ZZ9HCC?:display_count=y&:origin=viz_share_link&:embed=y
    # Hawai'i https://public.tableau.com/shared/7TCBHC568?:display_count=y&:origin=viz_share_link&:embed=y
    
    # baseurl  = "https://public.tableau.com/views/"
    # viewPath = "HawaiiCOVID-19-VaccinationDashboard/VACCINESBYCOUNTY"
    baseurl = "https://public.tableau.com/shared/"
    viewPath = "7TCBHC568"

    def fetch(self):
        
        return self.get_tableau_view(self.source_url)

    def normalize(self, data):
        return data

