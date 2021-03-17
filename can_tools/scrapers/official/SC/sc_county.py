import pandas as pd
import us
import camelot
import numpy as np
import requests
from bs4 import BeautifulSoup
from can_tools.scrapers.base import CMU
from can_tools.scrapers.official.base import StateDashboard, TableauDashboard

class SCVaccineCounty(StateDashboard):
    source = "https://scdhec.gov/covid19/covid-19-vaccine-allocation"
    source_name = "South Carolina Department of Health and Environmental Control"

    base_url = "https://scdhec.gov/sites/default/files/media/document/"
    state_fips = int(us.states.lookup("South Carolina").fips)
    has_location = False
    location_type = "county"
    janssen_format = "Janssen-Vaccine-Allocation-%s.pdf"
    moderna_format = "Moderna-Vaccine-Allocation-%s.pdf"
    pfizer_format = "Pfzier-BioNTech-Vaccine-Allocation-%s.pdf"
        
    def _url_for_vaccine_date(self, vaccine, soup):
        # Find the correct donwload link for vaccine and date
        url = soup.find('a', 
            title=lambda x: x and ('%s - %s' % (self.execution_dt.strftime('%B %-d, %Y'), vaccine.capitalize())) in x )
        if url is None:
            return url
        # get the href and combine with hostname
        url = "https://scdhec.gov" + url['href'].strip()
        return url

    def _fetch_vaccine(self, vaccine, soup):
        url = self._url_for_vaccine_date(vaccine,soup)
        if url is None or requests.head(url).status_code != 200:
            return []
        return camelot.read_pdf(url, pages="all", flavor='stream')

    def fetch(self):
        # First request the webpage with the download links
        list_url = "https://scdhec.gov/covid19/covid-19-vaccine-allocation"
        res = requests.get(list_url)
        # Get the soup
        soup = BeautifulSoup(res.text, 'html.parser')

        return {
            'moderna' : self._fetch_vaccine('moderna', soup),
            'pfizer': self._fetch_vaccine('pfizer', soup),
            "janssen": self._fetch_vaccine('jenssen', soup)
        } 

    def _normalize_one_dose(self, data, vaccine_name):
        dfs = []
        for d in data:
            
            data_start_row = d.df.index[d.df[0] == 'Providers'][0] + 1 # Get row where data starts
            df = d.df.iloc[data_start_row:]

            df.columns = [
                "Provider",
                "City",
                "location_name",
                "First-Doses Received",
                "First-Doses Distributed",
                "First-Doses Administered",
                "First-Doses Utlization"
               
            ]
            # Drop all columns that don't have city and county
            df = df.loc[~(df.City == "")]
            df = df.loc[~(df.location_name == "")]
            # Replace "--" with 0
            df = df.replace("--", 0)
            # Remove all commas

            df['First-Doses Administered'] = pd.to_numeric(df['First-Doses Administered'], errors="coerce")
            if not df.empty:
                dfs.append(df)
        if len(dfs) == 0:
            return
        df = pd.concat(dfs)
        keep_cols = ['location_name', "First-Doses Administered"]
        # group by county
        gbc = df[keep_cols].groupby("location_name")
        df = gbc.sum()
        
        crename = {
            
            'First-Doses Administered': CMU(
                category=f"{vaccine_name}_vaccine_initiated",
                measurement="cumulative",
                unit="people"
            )
        }
        
        melted = df.reset_index().melt(id_vars=['location_name'], value_vars=crename.keys())
        
        out = self.extract_CMU(melted, crename)
        out['vintage'] = self._retrieve_vintage()
        out['dt'] = self._retrieve_dt('US/Eastern')
        return out.reset_index()

    def _normalize_two_dose(self, data, vaccine_name):
        dfs = []
        init_dfs = []
        for d in data:
            init_dfs.append(d.df)
        # remove duplicate dfs
        init_dfs = [init_dfs[x] for x, _ in enumerate(init_dfs) if init_dfs[x].equals(init_dfs[x-1]) is False]

        for d in init_dfs:
            # Replace empty strings with NaN
            df = d.replace("", np.nan)

            # df[0][first_valid_index] should either be "Providers" or "Providers\nCity"
            data_start_row = df[0].first_valid_index()

            # # Only take rows where data starts 
            # df = d.iloc[(data_start_row + 1):]
            
            # # Replace "--" with 0
            # df = df.replace("--", 0)

            # TODO: Find column number where df[col_index][first_valid_index] == 'County'     
            first_valid_row = df.iloc[data_start_row]
            county_col_num = first_valid_row[first_valid_row == 'County'].index[0]

            # Drop all rows that don't have a county name
            # Drop all rows that don't have a county name
            # These are usually parent rows that sum up all rows below it
            df = df.loc[~(df[county_col_num] == np.nan)]

            # Name columns
            cols = []
            # fill cols up to county col
            for i in range(county_col_num-1):
                cols.append(i)
            
            # TODO: Ensure columns are correct
            df.columns = [
                "Provider",
                "City",
                "location_name",
                "First-Doses Received",
                "First-Doses Distributed",
                "First-Doses Administered",
                "First-Doses Utlization",
                "Second-Doses Received",
                "Second-Doses Distributed",
                "Second-Doses Administered",
                "Second-Doses Utlization"
            ]

            df['First-Doses Administered'] = pd.to_numeric(df['First-Doses Administered'], errors="coerce")
            df['Second-Doses Administered'] = pd.to_numeric(df['Second-Doses Administered'], errors="coerce")
            if not df.empty:
                dfs.append(df)

        if len(dfs) == 0:
            return pd.DataFrame()
        df = pd.concat(dfs)
        keep_cols = ['location_name', "First-Doses Administered", "Second-Doses Administered"]
        # group by county
        gbc = df[keep_cols].groupby("location_name")
        df = gbc.sum()
        
        crename = {
            
            'First-Doses Administered': CMU(
                category=f"{vaccine_name}_vaccine_initiated",
                measurement="cumulative",
                unit="people"
            ),
            'Second-Doses Administered': CMU(
                category=f"{vaccine_name}_vaccine_completed",
                measurement="cumulative",
                unit="people"
            ),

        }
        
        melted = df.reset_index().melt(id_vars=['location_name'], value_vars=crename.keys())
        
        out = self.extract_CMU(melted, crename)
        out['vintage'] = self._retrieve_vintage()
        out['dt'] = self._retrieve_dt('US/Eastern')
        return out.reset_index()

    def normalize(self, data):
        pfizer = self._normalize_two_dose(data['pfizer'], 'pfizer')
        moderna = self._normalize_two_dose(data['moderna'], 'moderna')
        janssen = self._normalize_one_dose(data['janssen'], 'janssen')

        return pd.concat([pfizer, moderna, janssen])

class SouthCarolinaTableauVaccineCounty(TableauDashboard):
    baseurl = "https://public.tableau.com/"
    viewPath = "COVIDVaccineDashboard/RECIPIENTVIEW"

    source = "https://scdhec.gov/covid19/covid-19-vaccination-dashboard"
    source_name = "South Carolina Department of Health and Environmental Control"

    base_url = "https://scdhec.gov/sites/default/files/media/document/"
    state_fips = int(us.states.lookup("South Carolina").fips)
    has_location = False
    location_type = "county"

    def fetch(self):
        return self.get_tableau_view()

    def normalize(self, data):
        return

