import pandas as pd
import requests
#for csv testing
import csv
import os
os.chdir("C:\\Users\\Sean McClure\\Documents\\GitHub\\can-scrapers\\can_tools\\scrapers\\official\\CDCVaccine")

"""
————————————————————————————————————————————————————————————————
Overall Numbers by State
Daily Snapshot -- cumulative
Census2019 = state pop
————————————————————————————————————————————————————————————————
"""
def __snapshot__():

    source = "https://covid.cdc.gov/covid-data-tracker/COVIDData/getAjaxData?id=vaccination_data"
    res = requests.get(source)
    if not res.ok:
        raise ValueError("uhm erhm uhhhh wellll")
    raw = res.json()
    df = pd.json_normalize(raw['vaccination_data'])
    df.to_csv("output.csv", index=False)
    print(df)

    return None

"""
————————————————————————————————————————————————————————————————
Moderna & Pfizer weekly by state etc

————————————————————————————————————————————————————————————————
"""
def _fetch(source):
    res = requests.get(source)
    if not res.ok:
        raise ValueError("wrong link lol")
    raw = res.json()   
    return pd.json_normalize(raw)

def __complete__():
    # link = "https://data.cdc.gov/resource/saz5-9hgg.json" ## PFIZER
    link = "https://data.cdc.gov/resource/b7pe-5nws.json" ## MODERNA
    
    df = _fetch(link) 
    # for name in df.columns:
    #     print(name)
    df.to_csv("output.csv", index=False)
    print(df)
    return None


version =  int(input("Select Method: \n1: Overall \n2: Complete\n"))
if version == 1:
    __snapshot__()
elif version == 2:
    __complete__()
