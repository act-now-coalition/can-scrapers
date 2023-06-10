import pandas as pd
import us
import os.path
from tqdm import tqdm
from can_tools.scrapers.official.base import TableauDashboard
from multiprocessing import Pool
from can_tools.scrapers import variables


class VirginiaVaccine(TableauDashboard):
    state_fips = int(us.states.lookup("VA").fips)
    source = "https://www.vdh.virginia.gov/coronavirus/covid-19-vaccine-summary/"
    source_name = "Virginia Department of Health"
    baseurl = "https://public.tableau.com"
    provider = "state"
    has_location = False
    location_type = "county"
    data_tableau_table = "Doses Administered by Administration FIPS"
    filterFunctionName = None
    viewPath = "VirginiaCOVID-19Dashboard-VaccineSummary/VirginiaCOVID-19VaccineSummary"

    variables = {
        "initiated": variables.INITIATING_VACCINATIONS_ALL,
        "complete": variables.FULLY_VACCINATED_ALL,
    }

    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:

        cols = {
            "SUM(Fully Vaccinated (Federal Doses count))-alias": "federal_complete",
            "SUM(At Least One Dose (Federal Doses count))-alias": "federal_initiated",
            "SUM(At Least One Dose (CVX=FedDosesLoc))-alias": "initiated",
            "SUM(Fully Vaccinated (CVX=FedDosesLoc))-alias": "complete",
            "ATTR(Locality Name)-alias": "location_name",
        }

        # VA reports federal doses in a separate column.
        # We combine the "standard" dose values with the federal dose values
        # in order to include all the doses.
        return (
            df.rename(columns=cols)
            # some counties have no federal doses, and the values are filled with %null%
            # replace with 0 so we can still aggregate.
            .replace("%null%", 0)
            .assign(
                complete=lambda row: pd.to_numeric(row["complete"])
                + pd.to_numeric(row["federal_complete"]),
                initiated=lambda row: pd.to_numeric(row["initiated"])
                + pd.to_numeric(row["federal_initiated"]),
                dt=self._retrieve_dt("US/Eastern"),
                vintage=self._retrieve_vintage(),
            )
            .pipe(self._reshape_variables, variable_map=self.variables)
        )


class VirginiaCountyVaccineDemographics(VirginiaVaccine):
    viewPath = "VirginiaCOVID-19Dashboard-VaccineDemographics/VirginiaCOVID-19VaccineDemographics"
    filterFunctionName = (
        "[federated.1a55gj51go17vw1cds8i50u4b8by].[none:Locality Name:nk]"
    )
    filterFunctionValue = None
    has_location = False
    location_type = "county"
    demographic_data = True

    secondaryFilterFunctionName = "[Parameters].[Parameter 1]"
    secondaryFilterValues = ["full", "one"]
    secondaryFilterValue = None

    def _fetch_county_dose(self, county_dose):
        self.secondaryFilterValue = county_dose[1]
        self.filterFunctionValue = county_dose[0]
        data = self.get_tableau_view()
        res = {}
        res["age"] = data["Vaccinations - Age Group"]
        res["sex"] = data["Vaccinations - Sex"]
        res["race"] = data["Vaccinations - Race Ethnicity"]
        return res

    def _fetch_multiprocess(self, func, i, n_processors):
        with Pool(processes=n_processors) as pool:
            return list(tqdm(pool.imap(func, i), total=len(i)))

    def fetch(self):
        counties = list(
            pd.read_csv(
                os.path.dirname(__file__) + "/../../../bootstrap_data/locations.csv"
            ).query(f"state == {self.state_fips} and name != 'Virginia'")["name"]
        )
        # Set up multithreading
        n_processors = 6
        args = []
        for county in counties:
            for dose in self.secondaryFilterValues:
                args.append([county, dose])
        res = self._fetch_multiprocess(self._fetch_county_dose, args, n_processors)
        return res

    def _normalize_sex(self, data):
        df = pd.concat(x["sex"] for x in data)
        df["dt"] = self._retrieve_dt()
        df = df.rename(
            columns={
                "AGG(Locality Name for String)-alias": "location_name",
                "SUM(Vaccine Status Count)-value": "value",
                "ATTR(KPI String)-alias": "category",
                "Sex-alias": "sex",
            }
        )
        df.category = df.category.str.replace(
            "People Fully Vaccinated", "total_vaccine_completed"
        )
        df.category = df.category.str.replace(
            "People with At Least One Dose", "total_vaccine_initiated"
        )
        df["age"] = "all"
        df["race"] = "all"
        df["ethnicity"] = "all"
        df.sex = df.sex.str.lower()
        return df[
            [
                "dt",
                "location_name",
                "category",
                "sex",
                "race",
                "ethnicity",
                "age",
                "value",
            ]
        ]

    def _normalize_age(self, data):
        df = pd.concat(x["age"] for x in data)
        df["dt"] = self._retrieve_dt()
        # I've seen the age column be labeled multiple ways, so rename all of them
        df = df.rename(
            columns={
                "AGG(Locality Name for String)-alias": "location_name",
                "SUM(Vaccine Status Count)-value": "value",
                "ATTR(KPI String)-alias": "category",
                "Age Group-alias": "age",
                "New Age Group-alias": "age",
                "Cases and Vaccine Age Group-alias": "age",
            }
        )
        df.category = df.category.str.replace(
            "People Fully Vaccinated", "total_vaccine_completed"
        )
        df.category = df.category.str.replace(
            "People with At Least One Dose", "total_vaccine_initiated"
        )
        df.age = df.age.str.replace("+", "_plus")
        df["race"] = "all"
        df["sex"] = "all"
        df["ethnicity"] = "all"
        return df[
            [
                "dt",
                "location_name",
                "category",
                "sex",
                "race",
                "ethnicity",
                "age",
                "value",
            ]
        ]

    def _normalize_race(self, data):
        df = pd.concat(x["race"] for x in data)
        df["dt"] = self._retrieve_dt()
        df = df.rename(
            columns={
                "AGG(Locality Name for String)-alias": "location_name",
                "SUM(Vaccine Status Count)-value": "value",
                "ATTR(KPI String)-alias": "category",
                "Race and Ethnicity-alias": "race",
            }
        )
        df.category = df.category.str.replace(
            "People Fully Vaccinated", "total_vaccine_completed"
        )
        df.category = df.category.str.replace(
            "People with At Least One Dose", "total_vaccine_initiated"
        )

        df.race = df.race.str.replace("White", "white")
        df.race = df.race.str.replace("Other Race", "other")
        df.race = df.race.str.replace("Native American", "ai_an")
        df.race = df.race.str.replace("Black", "black")
        df.race = df.race.str.replace("Latino", "latino")
        df.race = df.race.str.replace(
            "Asian or Pacific Islander", "asian_or_pacific_islander"
        )
        df["age"] = "all"
        df["sex"] = "all"
        df["ethnicity"] = "all"
        return df[
            [
                "dt",
                "location_name",
                "category",
                "sex",
                "race",
                "ethnicity",
                "age",
                "value",
            ]
        ]

    def normalize(self, data):
        age_df = self._normalize_age(data)
        race_df = self._normalize_race(data)
        sex_df = self._normalize_sex(data)
        out = pd.concat([age_df, race_df, sex_df])
        out["measurement"] = "cumulative"
        out["unit"] = "people"
        # out["category"] = "vaccine"
        out["vintage"] = self._retrieve_vintage()
        return out
