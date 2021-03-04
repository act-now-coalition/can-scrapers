from can_tools.scrapers import CMU

crename = {
    "Total Doses Allocated": CMU(
        category="total_vaccine_allocated",
        measurement="cumulative",
        unit="doses",
    ),
    "Vaccine Doses Administered": CMU(
        category="total_vaccine_doses_administered",
        measurement="cumulative",
        unit="doses",
    ),
    "People Vaccinated with at least One Dose": CMU(
        category="total_vaccine_initiated",
        measurement="cumulative",
        unit="people",
    ),
    "People Fully Vaccinated": CMU(
        category="total_vaccine_completed",
        measurement="cumulative",
        unit="people",
    ),
}
