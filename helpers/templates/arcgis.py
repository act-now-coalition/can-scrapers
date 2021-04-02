import us

from can_tools.scrapers.official.base import ArcGIS


class {ScraperName}(ArcGIS):
    """
    Fetch county level covid data from an ARCGIS dashboard

    Example implementations:
      * `can_tools/scrapers/official/FL/fl_state.py`
      * `can_tools/scrapers/official/GA/ga_vaccines.py`
    """

    ARCGIS_ID = {Insert the ARCGIS ID here}
    has_location = {True if contains fips, False if contains location names}
    location_type = {What kind of location is reported ('county', 'state'...)}
    state_fips = int(us.states.lookup({State name here}).fips)
    source = {Link to page with ArcGIS dashboard}

    def fetch(self):
        return self.get_all_jsons({ArcGIS Service}, {sheet}, {srvid})

    def normalize(self, data):
        df = self.arcgis_jsons_to_df(data)

        # TODO: Manipulate the data

        crename = {}

        # Extract category information and add other variable context
        out = self.extract_scraper_variables(out, crename)

        cols_to_keep = [
            "vintage",
            "dt",
            "location",
            "category",
            "measurement",
            "unit",
            "age",
            "race",
            "ethnicity",
            "sex",
            "value",
        ]

        return out.loc[:, cols_to_keep]
