from can_tools.scrapers.official import CDCCovidDataTracker, CDCVaccineTotal, CDCVaccineModerna, CDCVaccinePfizer 
import pandas as pd 

scraper = CDCVaccinePfizer()
x = scraper.normalize(scraper.fetch())
# print(x)
for col in x.columns:
    print(col)