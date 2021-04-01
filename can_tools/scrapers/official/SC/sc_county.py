import pandas as pd
import us
import camelot
import numpy as np
import requests
from bs4 import BeautifulSoup
from can_tools.scrapers.base import CMU
from can_tools.scrapers.official.base import StateDashboard, TableauDashboard
from datetime import timedelta

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
    data_date = None

    def _url_for_vaccine_date(self, vaccine, soup):
        # Find the correct donwload link for vaccine and date
        # if no data for current date, get most recent date
        url = None
        check_date = self.execution_dt
        while url is None:
            url = soup.find(
                "a",
                title=lambda x: x
                and (
                    "%s - %s"
                    % (check_date.strftime("%B %-d, %Y"), vaccine.capitalize()[0:1]) # check first two letters
                )
                in x,
            )
            if url is None:
                check_date = check_date - timedelta(days=1)

        self.data_date = check_date        
        if url is None:
            return url
        # get the href and combine with hostname
        url = "https://scdhec.gov" + url["href"].strip()
        return url

    def _fetch_vaccine(self, vaccine, soup):
        url = self._url_for_vaccine_date(vaccine, soup)
        if url is None or requests.head(url).status_code != 200:
            
            return []
        
        # print(f"getting {vaccine} data at url {url}")
        return camelot.read_pdf(
            url, 
            pages="all", 
            flavor="lattice", 
            process_background=True,
            strip_text=',',
            line_scale=40
        )

    def fetch(self):
        # First request the webpage with the download links
        list_url = "https://scdhec.gov/covid19/covid-19-vaccine-allocation"
        res = requests.get(list_url)
        # Get the soup
        soup = BeautifulSoup(res.text, "html.parser")

        return {
            "moderna": self._fetch_vaccine("moderna", soup),
            "pfizer": self._fetch_vaccine("pfizer", soup),
            "janssen": self._fetch_vaccine("janssen", soup),
        }

    def _extract_dfs(self, data):
        dfs = []
        for d in data:
            dfs.append(d.df)
        return dfs
   
    def _clean_names(self, name):
        ix = name.find("\n")
        if ix != -1:
            return name[ix+1:]
        else :
            return name

    def _normalize_one_dose(self, vaccine_data, vaccine_name):
        dfs = self._extract_dfs(vaccine_data)
        res = []
        for ix, d in enumerate(dfs):
            # Clean data
             if len(df.columns) != 11: 
                print(f"\t{vaccine_name} #{ix} doesn't have right number of columns!")
            df = d.replace("", np.nan).replace("--", 0)
            # Remove first empty row
            df = df[1:]
            # Check if first column parsed correctly
            if "Providers" != df[0][1]:
                # TODO: Replace the rows were col 1 is NaN with the split/expand
                print(f"Couldn't parse {vaccine_name} #{ix}!")
                continue
            # Set first row as column names
            df.columns = df.iloc[0]
            # Remove first row
            df = df[1:]
            # Drop all rows where County == np.NaN
            df = df.loc[~df.County.isna()]
            # Drop all rows where county was changed from '--' to 0
            df = df.loc[~(df.County == 0)]

            df.columns = [
                'Providers',
                'City',
                "location_name",
                "First-Doses Received",
                "First-Doses Distributed",
                "First-Doses Administered",
                "First-Doses Utlization"
            ]

            res.append(df)
        if len(res) == 0:
            return pd.DataFrame()
        df = pd.concat(res)
        keep_cols = [
            "location_name",
            "First-Doses Administered",
        ]
        df['First-Doses Administered'] = pd.to_numeric( df['First-Doses Administered'], errors='coerce')
        # group by county
        df['location_name'] = df['location_name'].apply(self._clean_names)
        gbc = df[keep_cols].groupby("location_name")
        df = gbc.sum()

        crename = {
            "First-Doses Administered": CMU(
                category=f"{vaccine_name}_vaccine_completed",
                measurement="cumulative",
                unit="people",
            ),
           
        }

        melted = df.reset_index().melt(
            id_vars=["location_name"], value_vars=crename.keys()
        )

        out = self.extract_CMU(melted, crename)

        non_counties = ['Totals', 'Totals:']
        out = out.query('location_name not in @non_counties')

        out['dt'] = self._retrieve_dt()
        out['vintage'] = self._retrieve_vintage()
        return out.drop(['variable'], axis="columns")
            

    def _remove_duplicates(self, data):
        init_dfs = []
        for d in data:
            init_dfs.append(d.df)
        # remove duplicate dfs
        init_dfs = [
            init_dfs[x]
            for x, _ in enumerate(init_dfs)
            if init_dfs[x].equals(init_dfs[x - 1]) is False
        ]

        return init_dfs



    def _normalize_two_dose(self, vaccine_data, vaccine_name):
        dfs = self._extract_dfs(vaccine_data)
        res = []
        for ix, d in enumerate(dfs):
            # Clean data
            df = d.replace("", np.nan).replace("--", 0)
            if len(df.columns) != 11: 
                print(f"\t{vaccine_name} #{ix} doesn't have right number of columns!")
            # Remove first empty row
            df = df[1:]
            # Check if first column parsed correctly
            if "Providers" != df[0][1]:
                # TODO: Replace the rows were col 1 is NaN with the split/expand
                print(f"Couldn't parse {vaccine_name} #{ix}!")
                continue
            # Set first row as column names
            df.columns = df.iloc[0]
            # Remove first row
            df = df[1:]
            # Drop all rows where County == np.NaN
            df = df.loc[~df.County.isna()]
            # Drop all rows where county was changed from '--' to 0
            df = df.loc[~(df.County == 0)]

            df.columns = [
                'Providers',
                'City',
                "location_name",
                "First-Doses Received",
                "First-Doses Distributed",
                "First-Doses Administered",
                "First-Doses Utlization",
                "Second-Doses Received",
                "Second-Doses Distributed",
                "Second-Doses Administered",
                "Second-Doses Utlization",
            ]

            res.append(df)
        
        if len(res) == 0:
            return pd.DataFrame()
        df = pd.concat(res)
        keep_cols = [
            "location_name",
            "First-Doses Administered",
            "Second-Doses Administered",
        ]
        df['First-Doses Administered'] = pd.to_numeric( df['First-Doses Administered'], errors='coerce')
        df['Second-Doses Administered'] = pd.to_numeric( df['Second-Doses Administered'], errors='coerce')
        # clean up county names
        df['location_name'] = df['location_name'].apply(self._clean_names)

        # group by county
        gbc = df[keep_cols].groupby("location_name")
        df = gbc.sum()

        crename = {
            "First-Doses Administered": CMU(
                category=f"{vaccine_name}_vaccine_initiated",
                measurement="cumulative",
                unit="people",
            ),
            "Second-Doses Administered": CMU(
                category=f"{vaccine_name}_vaccine_completed",
                measurement="cumulative",
                unit="people",
            ),
        }

        melted = df.reset_index().melt(
            id_vars=["location_name"], value_vars=crename.keys()
        )

        out = self.extract_CMU(melted, crename)

        non_counties = ['Totals', 'Totals:']
        out = out.query('location_name not in @non_counties')
        out['dt'] = self.data_date
        out['vintage'] = self._retrieve_vintage()
        return out.drop(['variable'], axis="columns")

    def normalize(self, data):
        print("Normalizing pfizer data")
        pfizer = self._normalize_two_dose(data["pfizer"], "pfizer")
        print("Normalizing moderna data")
        moderna = self._normalize_two_dose(data["moderna"], "moderna")
        print("Normalizing janssen data")
        janssen = self._normalize_one_dose(data["janssen"], "janssen")

        return pd.concat([pfizer, moderna, janssen])

