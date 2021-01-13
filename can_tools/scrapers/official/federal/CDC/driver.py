from can_tools.scrapers.official import (
    CDCCovidDataTracker,
    CDCVaccineModerna,
    CDCVaccinePfizer,
)
import pandas as pd
import csv
import sys
import tabulate

if len(sys.argv) == 1:
    print("no args supplied. bye")
    quit()

prompt = sys.argv[1]
if prompt == "moderna":
    scraper = CDCVaccineModerna()
elif prompt == "pfizer":
    scraper = CDCVaccinePfizer()
else:
    print("type correctly lol. bye")
    quit()

df = scraper.normalize(scraper.fetch())
print(df)
# df = df.drop(columns={"value", "location","age","sex","race","ethnicity","dt","vintage"})
# print(df.drop_duplicates().to_markdown(index=False))
# for val in df["location"].unique():
#     print(val)

if sys.argv[len(sys.argv) - 1] == "csv":
    print("\nwriting to csv...")
    fl = prompt + ".csv"
    df.to_csv(fl, index=True)
    print("done +++")
