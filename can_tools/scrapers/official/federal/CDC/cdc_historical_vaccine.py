import pandas as pd
from can_tools.scrapers import variables
from can_tools.scrapers.official.base import ETagCacheMixin, FederalDashboard


class CDCCountyVaccine2(FederalDashboard, ETagCacheMixin):
    has_location = True
    location_type = "county"
    source = "https://data.cdc.gov/Vaccinations/COVID-19-Vaccinations-in-the-United-States-County/8xkx-amqh"
    source_name = "Centers for Disease Control and Prevention"
    provider = "cdc2"
    csv_url = "https://data.cdc.gov/api/views/8xkx-amqh/rows.csv?accessType=DOWNLOAD"

    variables = {
        "Administered_Dose1_Recip": variables.INITIATING_VACCINATIONS_ALL,
        "Series_Complete_Yes": variables.FULLY_VACCINATED_ALL,
    }

    # Send URL and filename that Mixin will use to check the etag
    def __init__(self, execution_dt: pd.Timestamp=None):
        ETagCacheMixin.initialize_cache(
            self,
            cache_url=self.csv_url,
            cache_file="cdc_county_vaccines.txt"
        )
        super().__init__(execution_dt=execution_dt)

    def fetch(self):
        return pd.read_csv(self.csv_url)

    def normalize(self, data: pd.DataFrame) -> pd.DataFrame:
        out = self._rename_or_add_date_and_location(
            data, location_column="FIPS", date_column="Date", locations_to_drop=["UNK"]
        )
        out = self._reshape_variables(out, self.variables, drop_duplicates=True)

        # an alaska county was renamed in 2014 and given a new fips: update from the old fips (2270) to the new (2158)
        # source: https://www.cdc.gov/nchs/data/data_acces_files/County-Geography.pdf
        out["location"] = out["location"].replace(2270, 2158)

        # CDC data uses zeroes instead of NaNs to represent missing data in many locations, creating locations with trailing zeroes.
        # The _reshape_variables method removes NaNs (missing data), and the pipeline will later fill those locations with the
        # next data-source, but this will not happen for the zero values (as 0 is usually a valid entry).
        # Since the zeroes here are essentially NaNs, we remove them.
        # This removes all zeroes in the dataframe, so leading zeroes are lost as well as trailing/any other zeroes.
        # A potential update would be to isolate and only remove the trailing zeroes for each location
        #
        # The location 78020 (St. John Island, VI) caused problems in the pipeline.
        # It was being assigned a location id "iso1:us#iso2:us-vi#fips:780120" (instead of "iso1:us#iso2:us-vi#fips:78020" with the correct FIPS)
        # which caused the update-and-promote-datasets to fail. Since we do not surface any data for VI and because we are still testing this data,
        # we chose to just block the location entirely
        #
        # example of pipeline failure: https://github.com/covid-projections/covid-data-model/runs/3052782694?check_suite_focus=true
        return out.query("location != 78020 and value != 0")
