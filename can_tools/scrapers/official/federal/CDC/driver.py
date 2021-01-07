from can_tools.scrapers.official import CDCCovidDataTracker, CDCVaccineTotal, CDCVaccineModerna, CDCVaccinePfizer, CDCCovidDataTracker 
import pandas as pd 
import csv
import sys

# scraper = CDCCovidDataTracker()
# x = scraper.normalize(scraper.fetch())

prompt = sys.argv[1]
if prompt == "moderna":
    scraper = CDCVaccineModerna()
elif prompt == "pfizer":
    scraper = CDCVaccinePfizer()
elif prompt == "total":
    scraper = CDCVaccineTotal()
else:
    print("type correctly lol")
    quit()

df = scraper.normalize(scraper.fetch())
print(df.head(20))
# for val in df["LongName"].unique():
#     print (val)

if sys.argv[len(sys.argv)-1] == "csv":
    print("\nwriting to csv...")    
    fl = prompt + ".csv"
    df.to_csv(fl, index=False)
    print("done ++")