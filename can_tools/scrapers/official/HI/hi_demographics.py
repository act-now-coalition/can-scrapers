import pandas as pd
import us

from can_tools.scrapers import HawaiiVaccineCounty, variables

pd.set_option("display.max_rows", 500)


class HawaiiVaccineRace(HawaiiVaccineCounty):
    viewPath = "HawaiiCOVID-19-VaccinationDashboard3/RACE"
    subsheet = "Race Progess"
    demographic = "race"
    demographic_col_name = "Race-alias"
    filterFunctionName = '[sqlproxy.0td6cgz0bpiy7x131qvze0jvbqr1].[none:County:nk]'

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
        return out.replace({"nhpi": "pacific_islander", "75+": "75_plus"})

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
    subsheet = "Age progress (with pharm)"
    demographic = "age"
    demographic_col_name = "Age Bins (match Pharm)-alias"
    filterFunctionName = "[sqlproxy.051olb00k3oo5j1gc5hz61tlutb7].[none:County:nk]"

    def normalize(self, data):
        dfs = []
        for county in self.counties:
            df = data[county][self.subsheet]
            df = df[
                [
                    "SUM(Population)-value",
                    "AGG(initiated doses (pharm+vams))-alias",
                    'AGG(completed doses (pharm + vams))-alias',
                    f"AGG(% initiating (pharm + vams))-alias",
                    f"AGG(% completing (pharm + vams))-alias",
                    self.demographic_col_name,
                ]
            ]
            df.columns = [
                "population",
                "total_vaccine_initiated",
                "total_vaccine_completed",
                "total_vaccine_initiated_percentage",
                "total_vaccine_completed_percentage",
                self.demographic,
            ]
            df = df[df[self.demographic] != 0]

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
