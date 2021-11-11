from can_tools.scrapers import ArizonaVaccineCounty
import pandas as pd

class ArizonaVaccineRace(ArizonaVaccineCounty):
    
    def fetch(self):
        dfs = {}
        for county_name, fips in [["PIMA", 4019]]:
            self.filterFunctionValue = county_name
            tables = self.get_tableau_view()
            people = tables["Number of People"]
            demographics = tables["Race (# people)"]
            dfs[fips] = {"people": people, "demographics": demographics}
        return dfs

    def normalize(self, data: pd.DataFrame) -> pd.DataFrame:
        
        for fips, values in data.items():
            total_people = values["people"]
            total_people = total_people.loc[total_people.someCondition=condition, 'A'].item()