from can_tools.scrapers import ScraperVariable

crename = {
    "Total Doses Allocated": ScraperVariable(
        category="total_vaccine_allocated",
        measurement="cumulative",
        unit="doses",
    ),
    "Vaccine Doses Administered": ScraperVariable(
        category="total_vaccine_doses_administered",
        measurement="cumulative",
        unit="doses",
    ),
    "People Vaccinated with at least One Dose": ScraperVariable(
        category="total_vaccine_initiated",
        measurement="cumulative",
        unit="people",
    ),
    "People Fully Vaccinated": ScraperVariable(
        category="total_vaccine_completed",
        measurement="cumulative",
        unit="people",
    ),
}
