import pandas as pd
import requests
import us

from can_tools.scrapers.base import CMU
from can_tools.scrapers.official.base import TableauDashboard


class IdahoVaccineCounty(TableauDashboard):
    has_location = False
    source = "https://coronavirus.idaho.gov/covid-19-vaccine/"
    state_fips = int(us.states.lookup("Idaho").fips)
    location_type = "county"
    baseurl = "https://public.tableau.com"
    viewPath = "COVID-19VaccineDataDashboard/Residence"

    def fetch(self) -> pd.DataFrame:
        res = self.get_tableau_view()
        data_url = res["meta"]["dataUrl"]
        root = data_url.split("bootstrapSession")
        new_url = root + "sessions/" + res["meta"]["sessionID"] + "/commands/tabdoc/categorical-filter-by-index"
        form_data = {
            "visualIdPresModel": '{"worksheet":"People County","dashboard":"Residence"}',
            "membershipTarget": "filter",
            "filterIndices": "[1]",
            "filterUpdateType": "filter-replace",
            "globalFieldName": "[federated.1unllaw1cp9kx41ckjo6d1cjr4ub].[none:SERIES COMPLETE:nk]",
        }

        new_resp = requests.post(new_url, data=form_data)

        # TODO: pick up here and do something with new_resp. It might not have data we need :(


    def normalize(self, data: pd.DataFrame) -> pd.DataFrame:
        return data
