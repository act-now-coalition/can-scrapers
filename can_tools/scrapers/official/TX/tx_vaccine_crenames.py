from can_tools.scrapers import CMU

crename = {
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
