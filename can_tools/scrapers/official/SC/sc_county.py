class SCVaccineCounty(TableauDashboard):
    has_location = False
    source = "https://scdhec.gov/covid19/covid-19-vaccination-dashboard"
    source_name = "S.C. Department of Health and Environmental Control"
    state_fips = int(us.states.lookup("South Carolina").fips)
    location_type = "county"
    baseurl = "https://public.tableau.com"
    viewPath = "COVIDVaccineDashboard/RECIPIENTVIEW"

    data_tableau_table = "County Table People Sc Residents"
    location_name_col = "Recipient County for maps-alias"
    timezone = "US/Eastern"

    # map wide form column names into CMUs
    cmus = {
        "Count of Doses": variables.TOTAL_DOSES_ADMINISTERED_ALL,
        "SC Residents with at least 1 Vaccine": variables.INITIATING_VACCINATIONS_ALL,
    }

    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        cols = {
            self.location_name_col: "location_name",
            "Measure Values-alias": "value",
            "Measure Names-alias": "variable",
        }
        df = df[cols.keys()]

        return (
            df.rename(columns=cols)
            .dropna()
            .query(
                "location_name not in ['%all%', 'nan'] and variable in @self.cmus.keys()"
            )
            .assign(
                dt=self._retrieve_dt(self.timezone),
                vintage=self._retrieve_vintage(),
                value=lambda x: pd.to_numeric(
                    x["value"].astype(str).str.replace(",", "")
                ),
            )
            .pipe(self.extract_CMU, cmu=self.cmus)
            .drop(["variable"], axis=1)
        )
