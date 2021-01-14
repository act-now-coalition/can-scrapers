import pandas as pd
from pandas.core.frame import DataFrame

from can_tools.scrapers.base import CMU
from can_tools.scrapers.official.base import TableauDashboard


class California(TableauDashboard):

    baseurl = "https://public.tableau.com"
    viewPath = "StateDashboard_16008816705240/6_1CountyTesting"
    filterFunctionName = '[federated.1vtltxr1fwdaou18i20yk06cu6zn].[none:COUNTY:nk]'

    counties = [
        'Alameda',
        'Alpine',
        'Amador',
        'Butte',
        'Calaveras',
        'Colusa',
        'Contra Costa',
        'Del Norte',
        'El Dorado',
        'Fresno',
        'Glenn',
        'Humboldt',
        'Imperial',
        'Inyo',
        'Kern',
        'Kings',
        'Lake',
        'Lassen',
        'Los Angeles',
        'Madera',
        'Marin',
        'Mariposa',
        'Mendocino',
        'Merced',
        'Modoc',
        'Mono',
        'Monterey',
        'Napa',
        'Nevada',
        'Orange',
        'Placer',
        'Plumas',
        'Riverside',
        'Sacramento',
        'San Benito',
        'San Bernardino',
        'San Diego',
        'San Francisco',
        'San Joaquin',
        'San Luis Obispo',
        'San Mateo',
        'Santa Barbara',
        'Santa Clara',
        'Santa Cruz',
        'Shasta',
        'Sierra',
        'Siskiyou',
        'Solano',
        'Sonoma',
        'Stanislaus',
        'Sutter',
        'Tehama',
        'Trinity',
        'Tulare',
        'Tuolumne',
        'Ventura',
        'Yolo',
        'Yuba',
    ]

    def fetch(self):
        return

    def normalize(self):
        df = DataFrame()
        for county in self.counties:
            currentCounty = DataFrame()
            currentCounty = self._get_testing(county)
            currentCounty["location_name"] = county
            df = pd.concat([df,currentCounty], axis=0).sort_values(["dt"])          

        df["vintage"] = self._retrieve_vintage()

        cols_to_keep = [
            "dt",
            "location_name",
            "category",
            "measurement",
            "unit",
            "age",
            "race",
            "sex",
            "value",
        ]

        return df.loc[:,cols_to_keep]

    def _get_testing(self,county):
        self.filterFunctionValue = county
        data = self._scrape_view()
        df = data["6.3 County Test - Line (2)"]
        df["dt"] = pd.to_datetime(df["DAY(Test Date)-value"])
        crename = {
            "SUM(Tests)-value": CMU(
                category="pcr_tests_total",
                measurement="new",
                unit="specimens",
            ),
        }
        df = (
            df.query("dt != '1970-01-01'")
            .melt(id_vars=["dt"], value_vars=crename.keys())
            .dropna()
        )
        df = self.extract_CMU(df, crename)

        return df