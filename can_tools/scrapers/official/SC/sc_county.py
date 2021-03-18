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

    def _uniquify(self, df_columns):
        seen = set()
      
        for item in df_columns:
            fudge = 1
            newitem = item
      
            while newitem in seen:
                fudge += 1
                newitem = "{}_{}".format(item, fudge)
        
            yield newitem
            seen.add(newitem)
      

    def _url_for_vaccine_date(self, vaccine, soup):
        # Find the correct donwload link for vaccine and date
        url = soup.find(
            "a",
            title=lambda x: x
            and (
                "%s - %s"
                % (self.execution_dt.strftime("%B %-d, %Y"), vaccine.capitalize()[0:1]) # check first two letters
            )
            in x,
        )
        if url is None:
            return url
        # get the href and combine with hostname
        url = "https://scdhec.gov" + url["href"].strip()
        return url

    def _fetch_vaccine(self, vaccine, soup):
        url = self._url_for_vaccine_date(vaccine, soup)
        if url is None or requests.head(url).status_code != 200:
            return []
        return camelot.read_pdf(url, pages="all", flavor="stream")

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

    def _rename_and_combine(self, old_df):
        df = old_df
        # df[0][first_valid_index] should either be "Providers" or "Providers\nCity"
        data_start_row = df[0].first_valid_index()

        headers = df.loc[0:data_start_row].fillna('').apply("".join).str.strip()
        # verify first entry contains Providers
        if not 'Providers' in headers[0]:
            # This is a bad table. Skip formatting. Likely doesn't have data
            # print(f"Skipped table number {count}. Bad format")
            return pd.DataFrame()

        df = df.iloc[data_start_row+1:]

        empty_headers_count = headers.value_counts()['']
        empty_indexes = []

        if empty_headers_count > 1 :
            empty_indexes = np.where(headers == "")[0]
            headers = list(self._uniquify(headers))

        # these are now unique 
        df.columns = headers

        # Check if we need to combine columns
        # Check if any columns don't have a name
        if empty_headers_count > 0:
            cols_to_drop = []
            for no_name_index in empty_indexes:
                print(f"\t working on column {no_name_index}")
                # Check if we need to combine columns
                if df.iloc[:, no_name_index - 1].isna().all(): # Data is missing out of named column
                    # ASSUMPTION: Data for this column must be in the no name column
                    # Get column name
                    col_name = df.columns[no_name_index - 1]
                    empty_col_name = df.columns[no_name_index]
                    # Create mask
                    mask = ((~df[col_name].isna()) ^ df[empty_col_name].isna()) | ~df[col_name].isna()
                    # Apply mask to create new column, combining other columns
                    new_col = df[col_name].loc[mask].append(df[empty_col_name].loc[~mask])
                    # Remove old columns and replace with new columns
                    df[col_name] = new_col
                    # Drop the no name column
                    cols_to_drop.append(empty_col_name)
                else:
                    print("Found and empty column, but don't know what to do with it")
            df = df.drop(cols_to_drop, axis=1)

        return df

    def _normalize_one_dose_df(self, df):
        # print(f"working on table # {count}")

        # Replace empty strings with NaN
        df = df.replace("", np.nan)
        # Replace "--" with 0
        df = df.replace("--", 0)
        
        # Combine header rows, and set column names
        df = self._rename_and_combine(df)

        # Drop all rows that don't have a county name
        # These are usually parent rows that sum up all rows below it
        df = df.loc[~(df.County.isna()), :]

        # drop empty columns
        good_col_mask = ((df != 0) & ~df.isna()).any(axis=0)
        df = df.loc[:, good_col_mask]   

        # rename columns
        county_col_loc = df.columns.get_loc('County')
        cols = []
        for i in range(county_col_loc):
            cols.append(i)
        for i in [
            'location_name',
            'First-Doses Received',
            'First-Doses Distrubted',
            'First-Doses Administered',
            'First-Doses Utilization'
        ]:
            cols.append(i)
        df.columns = cols

        df["First-Doses Administered"] = pd.to_numeric(
            df["First-Doses Administered"], errors="coerce"
        )

        df = df[[
            'location_name',
            'First-Doses Administered',
        ]]
        return df

    def _normalize_one_dose(self, vaccine_data, vaccine_name):
        dfs = []
        init_dfs = []
        for d in vaccine_data:
            init_dfs.append(d.df)
        # remove duplicate dfs
        init_dfs = [
            init_dfs[x]
            for x, _ in enumerate(init_dfs)
            if init_dfs[x].equals(init_dfs[x - 1]) is False
        ]
        count = 0
        for d in init_dfs:
            print(f"working on df # {count}")
            count = count + 1
            df = self._normalize_one_dose_df(d)
            if not df.empty:
                dfs.append(df)

        if len(dfs) == 0:
            return pd.DataFrame()
        df = pd.concat(dfs)
        keep_cols = [
            "location_name",
            "First-Doses Administered",
        ]
        # group by county
        gbc = df[keep_cols].groupby("location_name")
        df = gbc.sum()

        crename = {
            "First-Doses Administered": CMU(
                category=f"{vaccine_name}_vaccine_initiated",
                measurement="cumulative",
                unit="people",
            ),
        }

        melted = df.reset_index().melt(
            id_vars=["location_name"], value_vars=crename.keys()
        )

        out = self.extract_CMU(melted, crename)
        out["vintage"] = self._retrieve_vintage()
        out["dt"] = self._retrieve_dt("US/Eastern")
        return out



    def _normalize_two_dose(self, vaccine_data, vaccine_name):
        dfs = []
        init_dfs = []
        for d in vaccine_data:
            init_dfs.append(d.df)
        # remove duplicate dfs
        init_dfs = [
            init_dfs[x]
            for x, _ in enumerate(init_dfs)
            if init_dfs[x].equals(init_dfs[x - 1]) is False
        ]
        count = 0
        for d in init_dfs:
            count = count + 1
            # Replace empty strings with NaN
            df = d.replace("", np.nan)

            # Replace "--" with 0
            df = df.replace("--", 0)
            
            # df[0][first_valid_index] should either be "Providers" or "Providers\nCity"
            data_start_row = df[0].first_valid_index()
            first_valid_row = df.iloc[data_start_row]
            # verify first entry contains Providers
            if not 'Providers' in first_valid_row[0]:
                # This is a bad table. Skip formatting
                print(f"Skipped table number {count}. Bad format")
                continue
            # Find column number where df[col_index][first_valid_index] == 'County'
            county_col_num = first_valid_row[first_valid_row == "County"].index[0]

            # Drop all rows that don't have a county name
            # These are usually parent rows that sum up all rows below it
            df = df.loc[~(df[county_col_num].isna()), :]

            # drop empty columns
            good_col_mask = ((df != 0) & ~df.isna()).any(axis=0)
            df = df.loc[:, good_col_mask]

            # Name columns
            cols = []            
            # fill cols up to county col
            for i in range(county_col_num):
                cols.append(i)
            # add correct column names
            cols_to_add = [
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
            for i in cols_to_add:
                cols.append(i)
            
            # Ensure columns are correct
            df.columns = cols

            # remove rows before actual data
            df = df.iloc[1:]       

            df["First-Doses Administered"] = pd.to_numeric(
                df["First-Doses Administered"], errors="coerce"
            )
            df["Second-Doses Administered"] = pd.to_numeric(
                df["Second-Doses Administered"], errors="coerce"
            )
            df = df[[
                'location_name',
                'First-Doses Administered',
                'Second-Doses Administered'
            ]]
            if not df.empty:
                dfs.append(df)

        if len(dfs) == 0:
            return pd.DataFrame()
        df = pd.concat(dfs)
        keep_cols = [
            "location_name",
            "First-Doses Administered",
            "Second-Doses Administered",
        ]
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
        out["vintage"] = self._retrieve_vintage()
        out["dt"] = self._retrieve_dt("US/Eastern")
        return out

    def normalize(self, data):
        pfizer = self._normalize_two_dose(data["pfizer"], "pfizer")
        moderna = self._normalize_two_dose(data["moderna"], "moderna")
        janssen = self._normalize_one_dose(data["janssen"], "janssen")

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
