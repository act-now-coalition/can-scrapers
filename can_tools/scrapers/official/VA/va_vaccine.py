import pandas as pd
import us
import os.path
from tqdm import tqdm
from can_tools.scrapers.base import CMU
from can_tools.scrapers.official.base import TableauDashboard
from multiprocessing import Pool


class VirginiaVaccine(TableauDashboard):
    state_fips = int(us.states.lookup("Virginia").fips)
    source = "https://www.vdh.virginia.gov/coronavirus/covid-19-vaccine-summary/"
    source_name = "Virginia Department of Health"
    baseurl = "https://vdhpublicdata.vdh.virginia.gov"
    provider = "state"
    has_location = True
    location_type = ""
    filterFunctionName = None
    viewPath = "VirginiaCOVID-19Dashboard-VaccineSummary/VirginiaCOVID-19VaccineSummary"

    def fetch(self):
        return self.get_tableau_view()

    def normalize(self, data) -> pd.DataFrame:
        rows = [
            (
                data["Vaccine One Dose"]["SUM(At Least One Dose)-alias"].iloc[0],
                data["Vaccine Fully Vacinated"]["SUM(Fully Vaccinated)-alias"].iloc[0],
                data["Vaccine Total Doses"]["SUM(Vaccine Count)-alias"].iloc[0],
                self.state_fips,
                "Virginia",
            )
        ]
        state_df = pd.DataFrame.from_records(
            rows,
            columns=[
                "totalHadFirstDose",
                "totalHadSecondDose",
                "totalDoses",
                "location",
                "location_name",
            ],
        )
        county_df = data["Doses Administered by Administration FIPS"].rename(
            columns={
                "SUM(Fully Vaccinated)-alias": "totalHadSecondDose",
                "SUM(At Least One Dose)-alias": "totalHadFirstDose",
                "SUM(Vaccine Count)-alias": "totalDoses",
                "Recipient FIPS - Manassas Fix-alias": "location",
                "ATTR(Locality Name)-alias": "location_name",
            }
        )

        df = pd.concat([state_df, county_df], axis=0)

        crename = {
            "totalHadFirstDose": CMU(
                category="total_vaccine_initiated",
                measurement="cumulative",
                unit="people",
            ),
            "totalHadSecondDose": CMU(
                category="total_vaccine_completed",
                measurement="cumulative",
                unit="people",
            ),
            "totalDoses": CMU(
                category="total_vaccine_doses_administered",
                measurement="cumulative",
                unit="doses",
            ),
        }
        df = df.melt(id_vars=["location"], value_vars=crename.keys()).dropna()
        df = self.extract_CMU(df, crename)

        df.loc[:, "value"] = pd.to_numeric(df["value"])
        df.loc[:, "location"] = df["location"].astype(int)
        df["location_type"] = "county"
        df.loc[df["location"] == 51, "location_type"] = "state"
        df["dt"] = self._retrieve_dt()
        df["vintage"] = self._retrieve_vintage()
        return df.drop(["variable"], axis="columns")


class VirginiaCountyVaccineDemographics(VirginiaVaccine):
    viewPath = "VirginiaCOVID-19Dashboard-VaccineDemographics/VirginiaCOVID-19VaccineDemographics"
    filterFunctionName = (
        "[federated.1a55gj51go17vw1cds8i50u4b8by].[none:Locality Name:nk]"
    )
    filterFunctionValue = None
    has_location = False
    location_type = "county"

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
        df = df.rename(
            columns={
                "AGG(Locality Name for String)-alias": "location_name",
                "SUM(Vaccine Status Count)-value": "value",
                "ATTR(KPI String)-alias": "category",
                "Age Group-alias": "age",
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
