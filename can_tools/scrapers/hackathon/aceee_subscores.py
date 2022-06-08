import pandas as pd
from can_tools.scrapers.hackathon.aceee import AceeCityReport, format_data
from typing import List, Dict
import functools as ft
import pathlib
import numpy as np

DATA_PATH = pathlib.Path(__file__).parent / 'data' / 'community_wide_initiatives'

#TODO: this will load on import--find another way to access this.
CITY_LOCATIONS = pd.read_csv(pathlib.Path(__file__).parent / 'data/aceee-summary.csv', usecols=["state", "city", "location"])

COMMUNITY_WIDE_INITIATIVES_TABLES = {
    "community_wide_climate_mitigation_and_energy_goals_scores": {"pages": "158-160", "table_breaker": "Table E2."},
    "equity_driven_climate_action": {"pages": "160-162", "skip_first": True},
    "clean_distributed_energy_resources": {"pages": "163-165", "table_breaker": "Table E4"},
    "heat_island_mitigation_goals": {"pages": "165-167", "skip_first": True}
}

BUILDING_POLICIES_TABLES = {
    "scores_for_energy_code_adoption": {"pages": "168-170", "table_breaker": "*Point av"},
    "scores_for_building_code_compliance_and_enforcement": {"pages": "170-172", "skip_first": True},
    # "scores_for_policies_targeting_existing_buildings": {"pages": "173-183"}
    # TODO: pull the rest... (including mostly text)
}

TRANSPORTATION_POLICIES = {
    "scores_for_sustainable_transportation_strategies": {"pages": "186-188", "skip_first": True},
    # TODO: fix "scores_for_location_efficiency": {"pages": "190-191", "table_breaker": "Table E"},
    "scores_for_mode_shift": {"pages": "191-193", "skip_first": True},
    "scores_for_public_transit": {"pages": "194-196", "table_breaker": "Table E13."},
    "scores_for_efficient_vehicles": {"pages": "196-198", "skip_first": True},
    # TODO: E14 Scores for sustainable freight
    "scores_for_equitable_transportation": {"pages": "200-202", "table_breaker": "ENERGY AND"}
}

ENERGY_AND_WATER_UTILITIES = {
    # "scores_for_energy_efficiency_efforts_of_energy_utilities": {"pages": "202-204", "skip_first": True},
    "scores_for_decarbonization_efforts_of_energy_utilities": {"pages": "205-207", "remove_last": True},
    # "scores_for_efficiency_efforts_of_water_utilities": {"pages": "207-209", "skip_first": True},
}



class AceeeSubScores(AceeCityReport):

    def get_comprehensive_component(self, table_objects: Dict[str, Dict] = COMMUNITY_WIDE_INITIATIVES_TABLES):
        """get data from city info table"""

        submetrics = []
        for table_name, table_object in table_objects.items():
            tables: List[pd.DataFrame] = []
            for table in self.fetch(table_object["pages"]):
                if table_object.get("table_breaker", "!!!!!") in table.iloc[0][0]:
                    continue                
                start_idx = table.index[table.iloc[:, 0] == "City"].values[0]
                col_data = table[:start_idx + 1]
                for col in col_data:
                    col_data[col] = col_data[col].str.cat(sep=' ').lower().strip().replace(" ", "_").replace("(", "").replace(")", "").replace("-", "_").replace("–", "_")
                cols = col_data.iloc[-1]
                tables.append(table[start_idx +1:])
            if table_object.get("skip_first"):
                tables = tables[1:]
            elif table_object.get("remove_last"):
                tables = tables[:-1]
            
            data = pd.concat(tables)
            data.columns = cols
            data = data.rename(columns=lambda c: f'total_{table_name}' if 'total' in c else c)
            data = format_data(data)
            data = data.merge(CITY_LOCATIONS, how="left", on="city")
            
            submetrics.append(data)
        
        output =  ft.reduce(lambda left, right: pd.merge(left, right, on=["city", "state", "location"], how="left"), submetrics)
        output = output.rename(columns={
            "district_energy_equity _related_0.5_pts": "district_energy_equity_related_0.5_pts",
            "district_energy_equity__related_0.5_pts": "district_energy_equity_related_0.5_pts",
            "microgrid_equity _related_0.5_pts": "microgrid_equity_related_0.5_pts",
            "solar_equity__related_0.5_pts": "solar_equity_related_0.5_pts",
            "solar_equity _related_0.5_pts": "solar_equity_related_0.5_pts",
            "microgrid_equity__related_0.5_pts": "microgrid_equity_related_0.5_pts",
            })
        output = output.replace('N/A',np.NaN)
        output.to_csv(pathlib.Path(__file__).parent / 'data' / 'submetrics' / "community_wide_initiatives.csv", index=False)

    