import pandas as pd
import us

from can_tools.scrapers import variables
from can_tools.scrapers.base import CMU
from can_tools.scrapers.official.base import StateDashboard


class CaliforniaVaccineCounty(StateDashboard):
    has_location = False
    source = (
        "https://data.chhs.ca.gov/dataset/vaccine-progress-dashboard/"
        "resource/130d7ba2-b6eb-438d-a412-741bde207e1c"
    )
    source_name = "California Health & Human Services Agency"
    state_fips = int(us.states.lookup("California").fips)
    location_type = "county"

    url = (
        "https://data.chhs.ca.gov/dataset/e283ee5a-cf18-4f20-a92c-ee94a2866ccd/resource/"
        "130d7ba2-b6eb-438d-a412-741bde207e1c/download/covid19vaccinesbycounty.csv"
    )

    variables = {
        "cumulative_fully_vaccinated": variables.FULLY_VACCINATED_ALL,
        "cumulative_at_least_one_dose": variables.INITIATING_VACCINATIONS_ALL,
        "cumulative_total_doses": variables.TOTAL_DOSES_ADMINISTERED_ALL,
    }

    def fetch(self) -> pd.DataFrame:
        return pd.read_csv(self.url)

    def normalize(self, data: pd.DataFrame) -> pd.DataFrame:
        non_counties = [
            # adding these twice b/c I've seen different capitalization on different days
            "All Ca Counties",
            "All Ca and Non-Ca Counties",
            "All CA Counties",
            "All CA and Non-CA Counties",
            "Outside California",
            "Unknown",
        ]
        data = self._rename_or_add_date_and_location(
            data,
            location_name_column="county",
            date_column="administered_date",
            location_names_to_drop=non_counties,
        )

        data = self._reshape_variables(data, self.variables)
        return data


class CaliforniaVaccineDemographics(CaliforniaVaccineCounty):
    source = (
        "https://data.chhs.ca.gov/dataset/vaccine-progress-dashboard/"
        "resource/71729331-2f09-4ea4-a52f-a2661972e146"
    )
    url = (
        "https://data.chhs.ca.gov/dataset/e283ee5a-cf18-4f20-a92c-ee94a2866ccd/"
        "resource/71729331-2f09-4ea4-a52f-a2661972e146/download/"
        "covid19vaccinesbycountybydemographic.csv"
    )

    variables = {
        "cumulative_fully_vaccinated": variables.FULLY_VACCINATED_ALL,
        "cumulative_at_least_one_dose": variables.INITIATING_VACCINATIONS_ALL,
        "fully_vaccinated": CMU(
            category="total_vaccine_completed",
            measurement="new",
            unit="people",
        ),
        "at_least_one_dose": CMU(
            category="total_vaccine_initiated",
            measurement="new",
            unit="people",
        ),
    }

    def normalize(self, data: pd.DataFrame) -> pd.DataFrame:
        data = self._rename_or_add_date_and_location(
            data,
            location_name_column="county",
            date_column="administered_date",
            location_names_to_drop=["Statewide"],
        )

        # split and format each demographic then re-concat
        dfs = []
        demos = {"age": "Age Group", "race": "Race/Ethnicity"}
        for k, v in demos.items():
            df = data.query("demographic_category == @v").rename(
                columns={"demographic_value": k}
            )
            df = self._reshape_variables(
                df, self.variables, skip_columns=[k], id_vars=[k]
            )
            dfs.append(df)

        out = pd.concat(dfs)
        out["race"] = out["race"].str.lower()
        return out.replace(
            {
                "65+": "65_plus",
                "native hawaiian or other pacific islander": "pacific_islander",
                "black or african american": "black",
                "american indian or alaska native": "ai_an",
                "other race": "other",
                "multiracial": "multiple",
            }
        )
