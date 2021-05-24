import pandas as pd
import us

from can_tools.scrapers import HawaiiVaccineCounty, variables

pd.set_option("display.max_rows", 500)


class HawaiiVaccineRace(HawaiiVaccineCounty):
    viewPath = "HawaiiCOVID-19-VaccinationDashboard3/RACE"
    subsheet = "Race Progess"
    demographic = "race"
    demographic_col_name = "Race-alias"

    variables = {
        "total_vaccine_initiated": variables.INITIATING_VACCINATIONS_ALL,
        "total_vaccine_completed": variables.FULLY_VACCINATED_ALL,
        "total_vaccine_initiated_percentage": variables.PERCENTAGE_PEOPLE_INITIATING_VACCINE,
        "total_vaccine_completed_percentage": variables.PERCENTAGE_PEOPLE_COMPLETING_VACCINE,
    }

    def _wrangle(self, df):
        out = self._reshape_variables(
            df,
            self.variables,
            id_vars=[self.demographic],
            skip_columns=[self.demographic],
        )
        out[self.demographic] = out[self.demographic].str.lower()
        out["dt"] = self._retrieve_dtm1d("US/Hawaii")
        return out.replace({"nhpi": "pacific_islander", "70+": "70_plus"})

    def normalize(self, data):
        dfs = []
        for county in self.counties:
            df = data[county][self.subsheet]
            df = df[
                [
                    "SUM(1 Dose Count (persons initiated))-alias",
                    "SUM(2 Dose Count (persons completed))-alias",
                    f"AGG(Race LOD % of pop (initiated))-alias",
                    f"AGG(Race LOD % of pop (completed))-alias",
                    self.demographic_col_name,
                ]
            ]
            df.columns = [
                "total_vaccine_initiated",
                "total_vaccine_completed",
                "total_vaccine_initiated_percentage",
                "total_vaccine_completed_percentage",
                self.demographic,
            ]
            df["location_name"] = county
            df = df[df[self.demographic] != 0]
            df["total_vaccine_initiated_percentage"] = (
                df["total_vaccine_initiated_percentage"] * 100
            )
            df["total_vaccine_completed_percentage"] = (
                df["total_vaccine_completed_percentage"] * 100
            )
            dfs.append(df)

        df = pd.concat(dfs)
        return self._wrangle(df)


class HawaiiVaccineAge(HawaiiVaccineRace):
    viewPath = "HawaiiCOVID-19-VaccinationDashboard3/AGE"
    subsheet = "New Age Progress"
    demographic = "age"
    demographic_col_name = "Age Bins (decades updated)-alias"

    def normalize(self, data):
        dfs = []
        for county in self.counties:
            df = data[county][self.subsheet]
            df = df[
                [
                    "SUM(Population)-value",
                    "SUM(1 Dose Count (persons initiated))-alias",
                    "AGG(AGE LOD % persons initiated)-alias",
                    "AGG(AGE LOD % persons completed)-alias",
                    self.demographic_col_name,
                ]
            ]
            df.columns = [
                "population",
                "total_vaccine_initiated",
                "total_vaccine_initiated_percentage",
                "total_vaccine_completed_percentage",
                self.demographic,
            ]
            df = df[df[self.demographic] != 0]

            # calculate total_vaccine_completed cumulative people
            df["total_vaccine_completed"] = (
                df["population"] * df["total_vaccine_completed_percentage"]
            )

            # make percentages from proportions
            df["total_vaccine_initiated_percentage"] = (
                df["total_vaccine_initiated_percentage"] * 100
            )
            df["total_vaccine_completed_percentage"] = (
                df["total_vaccine_completed_percentage"] * 100
            )
            df["location_name"] = county
            dfs.append(df)

        df = pd.concat(dfs)
        return self._wrangle(df)
