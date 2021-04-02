import pandas as pd
import us

from can_tools.scrapers.base import ScraperVariable
from can_tools.scrapers.official.base import TableauDashboard
from can_tools.scrapers.official.WI.wi_county_vaccine import WisconsinVaccineCounty


class WisconsinVaccineAge(WisconsinVaccineCounty):
    data_tableau_table = "Age vax/unvax County"
    # age does not report missing/unknown entries
    missing_tableau_table = ""
    location_name_col = "AGG(Geography TT)-alias"
    location_type = "state"

    # map wide form column names into ScraperVariables
    cmus = {
        "SUM(Initiation or completed count for TT)-alias": ScraperVariable(
            category="total_vaccine_initiated",
            measurement="cumulative",
            unit="people",
        )
    }

    def _get_demographic(
        self, df: pd.DataFrame, demo: str, demo_col_name: str
    ) -> pd.DataFrame:
        """
        description: a general "normalize" function to avoid extra/copied code
                     each demographic uses this in its respective normalize

        params:
            demo: the demographic as labeled according to ScraperVariable (age,sex,race, etc...)
            demo_col_name: the name of the demographic column from the fetched data

        returns: normalized data in long format
        """

        # county names (converted to title case)
        df["location_name"] = df[self.location_name_col].str.title()
        # fix county names
        df = df.replace(
            {"location_name": {"St Croix": "St. Croix", "Fond Du Lac": "Fond du Lac"}}
        )

        # parse out data columns
        value_cols = list(set(df.columns) & set(self.cmus.keys()))
        assert len(value_cols) == len(self.cmus)

        df = (
            df.melt(id_vars=[demo_col_name, "location_name"], value_vars=value_cols)
            .dropna()
            .assign(
                dt=self._retrieve_dt(self.timezone),
                vintage=self._retrieve_vintage(),
                value=lambda x: pd.to_numeric(
                    x["value"].astype(str).str.replace(",", "")
                ),
            )
            .pipe(self.extract_scraper_variables, cmu=self.cmus)
        )
        df[demo] = df[demo_col_name]
        return df.drop(["variable", demo_col_name], axis=1)

    def fetch(self) -> pd.DataFrame:
        if self.missing_tableau_table:
            # extract both data table and missing data table
            dfs = [
                self.get_tableau_view().get(table)
                for table in [self.data_tableau_table, self.missing_tableau_table]
            ]
            return pd.concat(dfs)
        else:
            return self.get_tableau_view()[self.data_tableau_table]

    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        df = self._get_demographic(df, "age", "Age-value")
        return df.replace({"age": {"65+": "65_plus"}})


class WisconsinVaccineRace(WisconsinVaccineAge):
    data_tableau_table = "Race vax/unvax county"
    missing_tableau_table = "Race missing county"

    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        df = self._get_demographic(df, "race", "Race-value")
        rm = {
            "1": "white",
            "2": "black",
            "3": "native_american",
            "4": "asian",
            "5": "other",
            "U": "unknown",
        }
        return df.replace({"race": rm})


class WisconsinVaccineSex(WisconsinVaccineAge):
    data_tableau_table = "Sex vax/unvax county"
    missing_tableau_table = "Sex missing county"

    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        df = self._get_demographic(df, "sex", "Sex-value")
        df["sex"] = df["sex"].str.lower()
        return df


class WisconsinVaccineEthnicity(WisconsinVaccineAge):
    data_tableau_table = "Ethnicity vax/unvax county"
    missing_tableau_table = "Ethnicity missing county"

    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        df = self._get_demographic(df, "ethnicity", "Ethnicity-value")
        df["ethnicity"] = df["ethnicity"].str.lower()
        return df
