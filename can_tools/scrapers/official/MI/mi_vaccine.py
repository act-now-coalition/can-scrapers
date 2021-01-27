import pandas as pd
import us

from can_tools.scrapers.base import CMU
from can_tools.scrapers.official.base import StateDashboard


class MichiganVaccineCounty(StateDashboard):
    has_location = False
    source = "https://www.michigan.gov/coronavirus/0,9753,7-406-98178_103214_103272-547150--,00.html"
    state_fips = int(us.states.lookup("Michigan").fips)
    url = "https://www.michigan.gov/documents/flu/Covid_Vaccine_Doses_Administered_710815_7.xlsx"
    location_type = "county"

    def fetch(self):
        return pd.read_excel(self.url, sheet_name="Doses Administered")

    def normalize(self, data: pd.DataFrame) -> pd.DataFrame:
        # date is written out in first column name
        data["variable"] = data["Vaccine Type"] + data["Dose Number"]
        data["variable"] = data["variable"].str.replace(" ", "")

        def _make_cmu(cat):
            return CMU(
                category=cat,
                measurement="cumulative",
                unit="people",
            )

        colnames = {
            "Person's Residence in County": "location_name",
            "Data as of": "dt",
            "Number of Doses": "value",
        }
        cmus = {
            "ModernaFirstDose": _make_cmu("moderna_vaccine_initiated"),
            "ModernaSecondDose": _make_cmu("moderna_vaccine_completed"),
            "PfizerFirstDose": _make_cmu("pfizer_vaccine_initiated"),
            "PfizerSecondDose": _make_cmu("pfizer_vaccine_completed"),
            "total_initiated": _make_cmu("total_vaccine_initiated"),
            "total_completed": _make_cmu("total_vaccine_completed"),
            "total": CMU(
                category="total_vaccine_doses_administered",
                measurement="cumulative",
                unit="doses",
            ),
        }
        not_counties = ["No County", "Detroit"]  # noqa

        # need to sum over all the possible facility types for distribution
        df = (
            data.rename(columns=colnames)
            .loc[:, ["location_name", "dt", "variable", "value"]]
            .query("location_name not in  @not_counties")
            .pivot_table(
                index=["dt", "location_name"],
                columns="variable",
                values="value",
                aggfunc="sum",
            )
            .fillna(0)
            .astype(int)
            .assign(
                total_initiated=lambda x: x.eval("ModernaFirstDose + PfizerFirstDose"),
                total_completed=lambda x: x.eval(
                    "ModernaSecondDose + PfizerSecondDose"
                ),
            )
            .assign(
                total=lambda x: x.eval("total_initiated + total_completed"),
            )
        )

        # now we need to reindex to fill in all dates -- fill missing with 0
        dates = pd.Series(df.index.get_level_values("dt")).agg(["min", "max"])
        new_index = pd.MultiIndex.from_product(
            [
                pd.date_range(*dates),
                df.index.get_level_values("location_name").unique(),
            ],
            names=["dt", "location_name"],
        )

        return (
            df.reindex(new_index, fill_value=0)  # fill in missing dates
            .sort_index()  # make sure we are sorted
            .unstack(
                level=["location_name"]
            )  # make index=dt, columns=[variable,loc_name]
            .cumsum()  # compute cumulative sum
            .stack(level=[0, 1])  # long form Series
            .rename("value")  # name the series
            .reset_index()  # convert to long form df
            .assign(  # fill fix value
                value=lambda x: pd.to_numeric(x.loc[:, "value"]),
                vintage=self._retrieve_vintage(),
            )
            .pipe(self.extract_CMU, cmu=cmus)  # extract CMUs
            .drop(["variable"], axis=1)  # drop variable
        )
