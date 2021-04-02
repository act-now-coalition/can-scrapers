"""Commonly used variables"""

from can_tools.scrapers import ScraperVariable

INITIATING_VACCINATIONS_ALL = ScraperVariable(
    category="total_vaccine_initiated",
    measurement="cumulative",
    unit="people",
)

FULLY_VACCINATED_ALL = ScraperVariable(
    category="total_vaccine_completed",
    measurement="cumulative",
    unit="people",
)

TOTAL_DOSES_ADMINISTERED_ALL = ScraperVariable(
    category="total_vaccine_doses_administered",
    measurement="cumulative",
    unit="doses",
)

PERCENTAGE_PEOPLE_INITIATING_VACCINE = ScraperVariable(
    category="total_vaccine_initiated",
    measurement="current",
    unit="percentage",
)

PERCENTAGE_PEOPLE_COMPLETING_VACCINE = ScraperVariable(
    category="total_vaccine_completed",
    measurement="current",
    unit="percentage",
)
