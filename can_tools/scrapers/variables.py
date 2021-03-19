"""Commonly used variables"""

from can_tools.scrapers import CMU


INITIATING_VACCINATIONS_ALL = CMU(
    category="total_vaccine_initiated",
    measurement="cumulative",
    unit="people",
)

FULLY_VACCINATED_ALL = CMU(
    category="total_vaccine_completed",
    measurement="cumulative",
    unit="people",
)

TOTAL_DOSES_ADMINISTERED_ALL = CMU(
    category="total_vaccine_doses_administered",
    measurement="cumulative",
    unit="doses",
)
