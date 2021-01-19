import pandas as pd
import tabula
import requests
import us

from can_tools.scrapers import CMU
from can_tools.scrapers.official.base import StateDashboard

class FloridaVaccine():
    has_location = False
    state_fips = us.states.lookup("Florida").fips
    location_type = "state"
    source = "source"

    def fetch(self):
        fetch_url = "http://ww11.doh.state.fl.us/comm/_partners/covid19_report_archive/vaccine/vaccine_report_latest.pdf"
        res = requests.get(fetch_url)
        if not res.ok:
            print("failure")

        return res.content

    def normalize(self, data):
        df = tabula.read_pdf("http://ww11.doh.state.fl.us/comm/_partners/covid19_report_archive/vaccine/vaccine_report_latest.pdf", 
        pages=2, area=[134,77,1172.16,792]) #get table by it's location in the PDF
        df = pd.concat(df)
        
        crename = {
            'First dose': CMU(
                category="total_vaccine_initiated",
                measurement="new",
                unit="people",
                ),
            'complete': CMU(
                category="total_vaccine_completed",
                measurement="new",
                unit="people",
                ),
            'vaccinatedle': CMU(
                category="total_vaccine_doses_administered",
                measurement="new",
                unit="doses",
                ),
            'First dose.1': CMU(
                category="total_vaccine_initiated",
                measurement="new",
                unit="people",
                ),
            'complete.1': CMU(
                category="total_vaccine_completed",
                measurement="new",
                unit="people",
                ),
            'vaccinated.1': CMU(
                category="total_vaccine_doses_administered",
                measurement="new",
                unit="doses",
                ),
        }
        
        return df

 
 


