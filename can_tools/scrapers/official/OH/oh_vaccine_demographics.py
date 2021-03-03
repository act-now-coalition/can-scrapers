import time
import pandas as pd
import us

from can_tools.scrapers.base import CMU
from can_tools.scrapers.official.base import TableauDashboard


class OHVaccineDemographics(TableauDashboard):
    has_location = False
    source = "https://coronavirus.ohio.gov/wps/portal/gov/covid-19/dashboards/covid-19-vaccine/covid-19-vaccination-dashboard"
    source_name = "Ohio Department of Health"
    state_fips = int(us.states.lookup("Ohio").fips)
    location_type = "county"
    baseurl = "https://public.tableau.com"
    viewPath = "VaccineAdministrationMetricsDashboard/PublicCountyDash"

    data_tableau_table = ""
    location_name_col = ""
    timezone = "US/Eastern"

    # map wide form column names into CMUs
    cmus = {
    }

    def fetch(self):
        self.setup_tableau_session()
        self.get_tableau_view()
        self.sess.headers["referer"] = self.full_url

        url = self.vizql_url + f"/sessions/{self.initial_response['sessionid']}/commands/tabdoc/set-parameter-value"
        data = {
            "globalFieldName": (None, "[Parameters].[Parameter 6]"),
            "valueString": (None, "Vaccine Completed**"),
            "useUsLocale": (None, "false"),
        }

        print("About to change to vaccine completed")
        res1 = self.sess.post(url=url, files=data)
        if not res1.ok:
            raise ValueError("Error setting tableau parameter to get completed data")
        time.sleep(10)
        completed_data = self.get_tableau_view()

        print("About to change to vaccine started")
        data["valueString"] = (None, "Vaccine Started*")
        res2 = self.sess.post(url=url, files=data)
        if not res2.ok:
            raise ValueError("Error setting tableau parameter to get completed data")
        time.sleep(10)
        started_data = self.get_tableau_view()

        return completed_data, started_data
        # return self.get_tableau_view()
