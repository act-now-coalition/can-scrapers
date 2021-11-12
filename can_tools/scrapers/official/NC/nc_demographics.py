from tableauscraper import TableauScraper

from can_tools.scrapers import variables
from can_tools.scrapers import NCVaccine

# This uses the tableauscraper module from https://github.com/bertrandmartel/tableau-scraping
# which "mimics" the actions on a dashboard (like filtering from dropdowns, clicking on maps etc.)
# The docs are pretty good and there are helpful examples there, but knowing what filters/parameters to
# use when and in what order can be unclear.

# In general, I usually go to the dashboard with the network inspector open, 
# navigate to the data we want to pull, and try to match the names of the calls to the TableauScraper filtering functions.
# E.g. a network call that sends a `set-parameter-value` corresponds to the workbook.GetParameter(...) method,
# and a `select` call corresponds to the `worksheet.select(...)` function.


class NCVaccineAge(NCVaccine):
    # map wide form column names into CMUs
    variables = {
        "": variables.PERCENTAGE_PEOPLE_INITIATING_VACCINE,
        "": variables.PERCENTAGE_PEOPLE_COMPLETING_VACCINE,
    }

    def fetch(self):
        engine = TableauScraper()
        engine.loads("https://public.tableau.com/views/NCDHHS_COVID-19_Dashboard_Vaccinations/VaccinationDashboard")
        engine = engine.getWorkbook()
        sp = engine.goToStoryPoint("2")
        wb = sp.setParameter("County", "Wake County")

        # i feel like with the set paramater that this should be returning data in the Race_county... tables
        # but nothing :/ 
        return wb.worksheets