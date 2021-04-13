import pandas as pd
import us
from tqdm import tqdm
from can_tools.scrapers.base import CMU
from can_tools.scrapers.official.base import TableauDashboard


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
    counties = [
        "Accomack",
        "Albemarle",
        "Alexandria",
        "Alleghany",
        "Amelia",
        "Amherst",
        "Appomattox",
        "Arlington",
        "Augusta",
        "Bath",
        "Bedford",
        "Bland",
        "Botetourt",
        "Bristol",
        "Brunswick",
        "Buchanan",
        "Buckingham",
        "Buena Vista City",
        "Campbell",
        "Caroline",
        "Carroll",
        "Charles City",
        "Charlotte",
        "Charlottesville",
        "Chesapeake",
        "Chesterfield",
        "Clarke",
        "Colonial Heights",
        "Covington",
        "Craig",
        "Culpeper",
        "Cumberland",
        "Danville",
        "Dickenson",
        "Dinwiddie",
        "Emporia",
        "Essex",
        "Fairfax",
        "Fairfax City",
        "Falls Church",
        "Fauquier",
        "Floyd",
        "Fluvanna",
        "Franklin City",
        "Franklin County",
        "Frederick",
        "Fredericksburg",
        "Galax",
        "Giles",
        "Gloucester",
        "Goochland",
        "Grayson",
        "Greene",
        "Greensville",
        "Halifax",
        "Hampton",
        "Hanover",
        "Harrisonburg",
        "Henrico",
        "Henry",
        "Highland",
        "Hopewell",
        "Isle of Wight",
        "James City",
        "King and Queen",
        "King George",
        "King William",
        "Lancaster",
        "Lee",
        "Lexington",
        "Loudoun",
        "Louisa",
        "Lunenburg",
        "Lynchburg",
        "Madison",
        "Manassas City",
        "Manassas Park",
        "Martinsville",
        "Mathews",
        "Mecklenburg",
        "Middlesex",
        "Montgomery",
        "Nelson",
        "New Kent",
        "Newport News",
        "Norfolk",
        "Northampton",
        "Northumberland",
        "Norton",
        "Nottoway",
        "Orange",
        "Page",
        "Patrick",
        "Petersburg",
        "Pittsylvania",
        "Poquoson",
        "Portsmouth",
        "Powhatan",
        "Prince Edward",
        "Prince George",
        "Prince William",
        "Pulaski",
        "Radford",
        "Rappahannock",
        "Richmond City",
        "Richmond County",
        "Roanoke City",
        "Roanoke County",
        "Rockbridge",
        "Rockingham",
        "Russell",
        "Salem",
        "Scott",
        "Shenandoah",
        "Smyth",
        "Southampton",
        "Spotsylvania",
        "Stafford",
        "Staunton",
        "Suffolk",
        "Surry",
        "Sussex",
        "Tazewell",
        "Virginia Beach",
        "Warren",
        "Washington",
        "Waynesboro",
        "Westmoreland",
        "Williamsburg",
        "Winchester",
        "Wise",
        "Wythe",
        "York",
    ]

    secondaryFilterFunctionName = "[Parameters].[Parameter 1]"
    secondaryFilterValues = ["full", "one"]
    secondaryFilterValue = None

    def fetch(self):
        res = {"age": [], "sex": [], "race": []}
        counts = tqdm(self.counties)
        for n in counts:
            for x in self.secondaryFilterValues:
                counts.set_description(f"Processing {n}")
                self.secondaryFilterValue = x
                self.filterFunctionValue = n
                data = self.get_tableau_view()
                res["age"].append(data["Vaccinations - Age Group"])
                res["sex"].append(data["Vaccinations - Sex"])
                res["race"].append(data["Vaccinations - Race Ethnicity"])
        return res

    def _normalize_sex(self, data):
        df = pd.concat(data["sex"])
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
        df = pd.concat(data["age"])
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
        df = pd.concat(data["race"])
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
        df.race = df.race.str.replace("Native American", "native_american")
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
        out["last_updated"] = self._retrieve_vintage()
        return out
