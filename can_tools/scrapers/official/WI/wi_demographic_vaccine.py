import pandas as pd
import us
import os
from can_tools.scrapers.official.base import TableauDashboard
from tableauscraper import TableauScraper as TS
from can_tools.scrapers.official.WI.wi_county_vaccine import WisconsinVaccineCounty
from can_tools.scrapers import variables, CMU


class WisconsinVaccineStateAge(WisconsinVaccineCounty):
    data_tableau_table = "Age vax/unvax County"
    # age does not report missing/unknown entries
    missing_tableau_table = ""
    location_name_col = "AGG(Geography TT)-alias"
    location_type = "state"

    # map wide form column names into CMUs
    cmus = {
        "SUM(Initiation or completed count for TT)-alias": CMU(
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
            demo: the demographic as labeled according to CMU (age,sex,race, etc...)
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
            .pipe(self.extract_CMU, cmu=self.cmus)
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


class WisconsinVaccineStateRace(WisconsinVaccineStateAge):
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


class WisconsinVaccineStateSex(WisconsinVaccineStateAge):
    data_tableau_table = "Sex vax/unvax county"
    missing_tableau_table = "Sex missing county"

    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        df = self._get_demographic(df, "sex", "Sex-value")
        df["sex"] = df["sex"].str.lower()
        return df


class WisconsinVaccineStateEthnicity(WisconsinVaccineStateAge):
    data_tableau_table = "Ethnicity vax/unvax county"
    missing_tableau_table = "Ethnicity missing county"

    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        df = self._get_demographic(df, "ethnicity", "Ethnicity-value")
        df["ethnicity"] = df["ethnicity"].str.lower()
        return df


class WisconsinVaccineCountyRace(WisconsinVaccineCounty):
    demographic_worksheet = "Race vax/unvax county"
    demographic_column = "Race-alias"
    demographic = "race"
    fullUrl = "https://bi.wisconsin.gov/t/DHS/views/VaccinesAdministeredtoWIResidents_16129838459350/VaccinatedWisconsin-County"

    variables = {
        "initiated": variables.INITIATING_VACCINATIONS_ALL,
        "completed": variables.FULLY_VACCINATED_ALL,
    }

    def _get_demographic_data(self, category: str, demo_col_name: str):
        # open connection to dashboard
        ts = TS()
        ts.loads(self.fullUrl)

        counties = list(
            pd.read_csv(
                os.path.dirname(__file__) + "/../../../bootstrap_data/locations.csv"
            )
            .query(f"state == 55 and location != 55")["name"]
            .replace(
                {
                    "La Crosse": "La crosse",
                    "Green Lake": "Green lake",
                    "Fond du Lac": "Fond du lac",
                    "St. Croix": "St croix",
                    "Eau Claire": "Eau claire",
                }
            )
        )

        # get worksheets and set filter type
        # returns initiated values by default -- specify if the filter needs to be changed to completed
        workbook = ts.getWorkbook()
        if category == "completed":
            workbook.setParameter(
                "Initiation or Completion",
                "Residents who have completed the vaccine series",
            )
        elif category not in ["completed", "initiated"]:
            raise ValueError(
                "category expected 'completed' or 'iniated' but received neither"
            )

        # get main sheet
        ws = workbook.getWorksheet("Map")
        dfs = []
        # for each county, extract data from subsheet and rename columns, append to list of dfs
        for c in counties:
            county_ws = ws.select("County", f"{c} County")
            df = county_ws.getWorksheet(self.demographic_worksheet).data
            df = df[
                [
                    demo_col_name,
                    "AGG(Geography TT)-alias",
                    "SUM(Initiation or completed count for TT)-alias",
                ]
            ]
            df = df.rename(
                columns={"SUM(Initiation or completed count for TT)-alias": category}
            )
            dfs.append(df)
        return pd.concat(dfs)

    def fetch(self):
        # get initiated and completed tables then concatenate
        init = self._get_demographic_data("initiated", self.demographic_column)
        complete = self._get_demographic_data("completed", self.demographic_column)
        return pd.merge(init, complete, how="left")

    def normalize(self, data):
        data = data.rename(columns={"AGG(Geography TT)-alias": "location_name"})
        data["location_name"] = (
            data["location_name"]
            .str.replace(" County", "")
            .replace("St Croix", "St. Croix")
        )
        data[self.demographic_column] = (
            data[self.demographic_column]
            .str.lower()
            .replace({"american indian": "ai_an"})
        )
        out = (
            data.melt(
                id_vars=["location_name", self.demographic_column],
                value_vars=self.variables.keys(),
            )
            .dropna()
            .assign(
                value=lambda x: pd.to_numeric(
                    x["value"].astype(str).str.replace(",", "")
                ),
            )
            .pipe(self.extract_CMU, cmu=self.variables)
            .drop(["variable"], axis=1)
        )
        out["dt"] = self._retrieve_dt(self.timezone)
        out["vintage"] = self._retrieve_vintage()

        return out.drop(columns={self.demographic}).rename(
            columns={self.demographic_column: self.demographic}
        )


class WisconsinVaccineCountySex(WisconsinVaccineCountyRace):
    demographic_worksheet = "Sex vax/unvax county"
    demographic_column = "Sex-alias"
    demographic = "sex"


class WisconsinVaccineCountyAge(WisconsinVaccineCountyRace):
    demographic_worksheet = "Age vax/unvax County"
    demographic_column = "Age-alias"
    demographic = "age"

    def normalize(self, data):
        df = super().normalize(data)
        return df.replace("65+", "65_plus")


class WisconsinVaccineCountyEthnicity(WisconsinVaccineCountyRace):
    demographic_worksheet = "Ethnicity vax/unvax county"
    demographic_column = "Ethnicity-alias"
    demographic = "ethnicity"
