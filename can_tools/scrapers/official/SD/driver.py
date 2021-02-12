from can_tools.scrapers.official import SouthDakotaCountyVaccine, MaineCountyVaccines

x = SouthDakotaCountyVaccine()
# x = MaineCountyVaccines()
print(x.fetch())
