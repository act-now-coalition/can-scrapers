import pandas as pd
import us

from can_tools.scrapers.base import CMU
from can_tools.scrapers.official.base import StateDashboard


class MichiganVaccineCounty(StateDashboard):
    has_location = False
    source = "https://www.michigan.gov/coronavirus/0,9753,7-406-98178_103214_103272-547150--,00.html"
    source_name = "State of Michican Official Website"
    state_fips = int(us.states.lookup("Michigan").fips)
    url = "https://www.michigan.gov/documents/coronavirus/Covid_Vaccine_Doses_Administered_718468_7.xlsx"
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
            "J&JFirstDose": _make_cmu("janssen_vaccine_completed"),
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
        not_counties = ["No County", "Non-Michigan Resident"]  # noqa

        # need to sum over all the possible facility types for distribution
        df = (
            data.rename(columns=colnames)
            .loc[:, ["location_name", "dt", "variable", "value"]]
            .query("location_name not in @not_counties")
            .assign(dt=lambda x: pd.to_datetime(x["dt"]))
            .pivot_table(
                index=["dt", "location_name"],
                columns="variable",
                values="value",
                aggfunc="sum",
            )
            .fillna(0)
            .astype(int)
            .assign(
                total_initiated=lambda x: x.eval("ModernaFirstDose + PfizerFirstDose")
                + x["J&JFirstDose"],
                total_completed=lambda x: x.eval("ModernaSecondDose + PfizerSecondDose")
                + x["J&JFirstDose"],
            )
            .assign(
                total=lambda x: x.eval("total_initiated + total_completed"),
            )
            .loc[:, cmus.keys()]
        )

        # Detroit data is reported separately from Wayne county. As detroit is not a real
        # county, combine data with Wayne county.
        is_wayne_county = df.index.get_level_values("location_name") == "Wayne"
        is_detroit = df.index.get_level_values("location_name") == "Detroit"

        renamed_detroit_data = df.loc[is_detroit, :].rename(
            index={"Detroit": "Wayne"}, level="location_name"
        )

        # verify that indices are the same so that when adding data frames
        # no values are dropped
        assert renamed_detroit_data.index.equals(df.loc[is_wayne_county, :].index)
        df.loc[is_wayne_county, :] += renamed_detroit_data

        # Drop detroit data
        df = df.loc[~is_detroit, :]

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


class MichiganVaccineCountyDemographics(MichiganVaccineCounty):
    variables = {
        "janssen_vaccine_completed": CMU(
            category="janssen_vaccine_completed", unit="people", measurement="new"
        ),
        "moderna_vaccine_completed": CMU(
            category="moderna_vaccine_completed", unit="people", measurement="new"
        ),
        "moderna_vaccine_initiated": CMU(
            category="moderna_vaccine_initiated", unit="people", measurement="new"
        ),
        "pfizer_vaccine_completed": CMU(
            category="pfizer_vaccine_completed", unit="people", measurement="new"
        ),
        "pfizer_vaccine_initiated": CMU(
            category="pfizer_vaccine_initiated", unit="people", measurement="new"
        ),
    }

    def fetch(self):
        return pd.read_excel(self.url, sheet_name="Doses Administered Demographics")

    def _clean_columns(self, df):
        df.Sex = df.Sex.replace("F", "female")
        df.Sex = df.Sex.replace("M", "male")
        df.Sex = df.Sex.replace("U", "unknown")
        df["Age Group"] = df["Age Group"].str.replace(" years", "")
        df["Age Group"] = df["Age Group"].str.replace("+", "_plus")
        df["Age Group"] = df["Age Group"].replace("missing", "unknown")
        return df

    def _pivot(self, df):
        out = df.pivot_table(
            columns=["Dose Number"],
            values="Doses Administered",
            index=["location_name", "Sex", "Age Group", "dt"],
            aggfunc=sum,
        ).rename_axis(None, axis=1)
        return out

    def _normalize_janssen(self, gb):
        df = gb.get_group(("J&J", "First Dose"))

        df["Dose Number"] = df["Dose Number"].str.replace(
            "First Dose", "janssen_vaccine_completed"
        )
        return df

    def _normalize_two_dose(self, gb):
        pfi1 = gb.get_group(("Pfizer", "First Dose"))
        pfi2 = gb.get_group(("Pfizer", "Second Dose"))
        mod1 = gb.get_group(("Moderna", "First Dose"))
        mod2 = gb.get_group(("Moderna", "Second Dose"))

        pfi1["Dose Number"] = pfi1["Dose Number"].str.replace(
            "First Dose", "pfizer_vaccine_initiated"
        )
        pfi2["Dose Number"] = pfi2["Dose Number"].str.replace(
            "Second Dose", "pfizer_vaccine_completed"
        )
        mod1["Dose Number"] = mod1["Dose Number"].str.replace(
            "First Dose", "moderna_vaccine_initiated"
        )
        mod2["Dose Number"] = mod2["Dose Number"].str.replace(
            "Second Dose", "moderna_vaccine_completed"
        )
        return pd.concat([pfi1, pfi2, mod1, mod2])

    def normalize(self, data):
        df = self._rename_or_add_date_and_location(
            data,
            location_name_column="Person's Residence in County",
            date_column="Week Ending Date",
        ).drop(
            columns=[
                "Person's Residence in Preparedness Region",
                "Person's Residence in Local Health Department Jurisdiction",
            ]
        )
        df = self._clean_columns(df)
        gbv = df.groupby(["Vaccine Type", "Dose Number"])
        two_dose = self._normalize_two_dose(gbv)
        jan = self._normalize_janssen(gbv)

        out = (
            pd.concat([two_dose, jan])
            .fillna(0)
            .reset_index()
            .rename(
                columns={
                    "Sex": "sex",
                    "Age Group": "age",
                    "Dose Number": "variable",
                    "Doses Administered": "value",
                }
            )
        )

        out = self.extract_CMU(
            out,
            self.variables,
            columns=[
                "measurement",
                "unit",
                "race",
                "ethnicity",
            ],
        )
        out["category"] = "vaccine"

        out["last_updated"] = self._retrieve_vintage()
        out["vintage"] = self._retrieve_vintage()
        locs_to_remove = ["No County", "Non-Michigan Resident"]
        cols = [
            "location_name",
            "dt",
            "sex",
            "age",
            "race",
            "ethnicity",
            "variable",
            "measurement",
            "unit",
            "value",
            "last_updated",
            "vintage",
        ]
        out = out.query("location_name not in @locs_to_remove")[cols]
        out = out.rename(columns={"variable": "category"})

        # combine Wayne county and Detroit records
        # combine rows that have every variable matching except value
        # rename Detroit entries to Wayne so that the rows we want get combined
        group_by = [c for c in out.columns if c not in ["value"]]
        out = out.replace("Detroit", "Wayne")
        out = out.groupby(group_by, as_index=False).aggregate({"value": "sum"})
        return out
