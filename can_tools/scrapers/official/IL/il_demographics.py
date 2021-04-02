import pandas as pd

from can_tools.scrapers.base import ScraperVariable, DatasetBaseNoDate
from can_tools.scrapers.official.IL.il_historical import IllinoisHistorical


class IllinoisDemographics(IllinoisHistorical, DatasetBaseNoDate):
    def _handle_demo_subset(
        self, df: pd.DataFrame, orig: str, final: str
    ) -> pd.DataFrame:
        "orig is demographic colname in df, final is what we want it to be"

        cats = {
            "count": ScraperVariable(
                category="cases",
                measurement="cumulative",
                unit="people",
            ),
            "tested": ScraperVariable(
                category="antigen_pcr_tests_total",
                measurement="cumulative",
                unit="unknown",
            ),
        }

        temp = "replaceme"

        return (
            df.rename(columns={orig: temp})
            .melt(value_vars=cats.keys(), id_vars=[temp])
            .assign(dt=self._retrieve_dt(), vintage=self._retrieve_vintage())
            .pipe(self.extract_scraper_variables, cats)
            .drop([final, "variable"], axis=1)
            .rename(columns={temp: final})
        )

    def get(self) -> pd.DataFrame:
        url = "https://idph.illinois.gov/DPHPublicInformation/api/COVID/GetCountyDemographics"
        js = self._get_js(url)

        # process age
        parts = []
        for county in js["county_demographics"]:
            name = county["County"]

            parts.append(
                self._handle_demo_subset(
                    pd.DataFrame(county["demographics"]["age"]),
                    "age_group",
                    "age",
                ).assign(county=name)
            )
            parts.append(
                self._handle_demo_subset(
                    pd.DataFrame(county["demographics"]["gender"]),
                    "description",
                    "sex",
                ).assign(county=name)
            )
            parts.append(
                self._handle_demo_subset(
                    pd.DataFrame(county["demographics"]["race"]),
                    "description",
                    "race",
                ).assign(county=name)
            )

        out = pd.concat(parts, ignore_index=True)
        out["race"] = out["race"].str.lower()
        out["sex"] = out["sex"].str.lower()
        out["age"] = out["age"].str.lower()
        return out
