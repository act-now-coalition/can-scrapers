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

crename_demographics = {
    "Doses Administered 50-64 years": CMU(
        category="total_vaccine_doses_administered",
        measurement="cumulative",
        unit="doses",
        age="50-64",
    ),
    "Doses Administered 16-49 years": CMU(
        category="total_vaccine_doses_administered",
        measurement="cumulative",
        unit="doses",
        age="16-49",
    ),
    "Doses Administered 65-79 years": CMU(
        category="total_vaccine_doses_administered",
        measurement="cumulative",
        unit="doses",
        age="65-79",
    ),
    "Doses Administered 80+ years": CMU(
        category="total_vaccine_doses_administered",
        measurement="cumulative",
        unit="doses",
        age="80_plus",
    ),
    "Doses Administered Unknown": CMU(
        category="total_vaccine_doses_administered",
        measurement="cumulative",
        unit="doses",
        age="unknown",
    ),
    "Doses Administered Total": CMU(
        category="total_vaccine_doses_administered",
        measurement="cumulative",
        unit="doses",
    ),
    "People Vaccinated with at least One Dose 16-49 years": CMU(
        category="total_vaccine_initiated",
        measurement="cumulative",
        unit="people",
        age="16-49",
    ),
    "People Vaccinated with at least One Dose 50-64 years": CMU(
        category="total_vaccine_initiated",
        measurement="cumulative",
        unit="people",
        age="50-64",
    ),
    "People Vaccinated with at least One Dose 65-79 years": CMU(
        category="total_vaccine_initiated",
        measurement="cumulative",
        unit="people",
        age="65-79",
    ),
    "People Vaccinated with at least One Dose 80+ years": CMU(
        category="total_vaccine_initiated",
        measurement="cumulative",
        unit="people",
        age="80_plus",
    ),
    "People Vaccinated with at least One Dose Unknown": CMU(
        category="total_vaccine_initiated",
        measurement="cumulative",
        unit="people",
        age="unknown",
    ),
    "People Vaccinated with at least One Dose Total": CMU(
        category="total_vaccine_initiated",
        measurement="cumulative",
        unit="people",
    ),
    "People Fully Vaccinated 16-49 years": CMU(
        category="total_vaccine_completed",
        measurement="cumulative",
        unit="people",
        age="16-49",
    ),
    "People Fully Vaccinated 50-64 years": CMU(
        category="total_vaccine_completed",
        measurement="cumulative",
        unit="people",
        age="50-64",
    ),
    "People Fully Vaccinated 65-79 years": CMU(
        category="total_vaccine_completed",
        measurement="cumulative",
        unit="people",
        age="65-79",
    ),
    "People Fully Vaccinated 80+ years": CMU(
        category="total_vaccine_completed",
        measurement="cumulative",
        unit="people",
        age="80_plus",
    ),
    "People Fully Vaccinated Unknown": CMU(
        category="total_vaccine_completed",
        measurement="cumulative",
        unit="people",
        age="unknown",
    ),
    "People Fully Vaccinated Total": CMU(
        category="total_vaccine_completed",
        measurement="cumulative",
        unit="people",
    ),
}
