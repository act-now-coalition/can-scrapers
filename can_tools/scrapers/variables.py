"""Commonly used variables"""

from can_tools.scrapers import CMU

INITIATING_VACCINATIONS_ALL: CMU = CMU(
    category="total_vaccine_initiated",
    measurement="cumulative",
    unit="people",
)

FULLY_VACCINATED_ALL: CMU = CMU(
    category="total_vaccine_completed",
    measurement="cumulative",
    unit="people",
)

TOTAL_DOSES_ADMINISTERED_ALL: CMU = CMU(
    category="total_vaccine_doses_administered",
    measurement="cumulative",
    unit="doses",
)

PERCENTAGE_PEOPLE_INITIATING_VACCINE: CMU = CMU(
    category="total_vaccine_initiated",
    measurement="current",
    unit="percentage",
)

PERCENTAGE_PEOPLE_COMPLETING_VACCINE: CMU = CMU(
    category="total_vaccine_completed",
    measurement="current",
    unit="percentage",
)

TOTAL_VACCINE_DISTRIBUTED: CMU = CMU(
    category="total_vaccine_distributed",
    measurement="cumulative",
    unit="doses",
)
