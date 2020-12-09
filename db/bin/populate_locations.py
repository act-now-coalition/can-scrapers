import pandas as pd

from can_tools.scrapers.uscensus.geo import USGeoBaseAPI

d = USGeoBaseAPI("state")
df_s = d.get()
df_s["location_type"] = 2

d = USGeoBaseAPI("county")
df_c = d.get()
df_c["location_type"] = 1

df = pd.DataFrame(pd.concat([df_s, df_c], ignore_index=True))
df["fullname"] = df["fullname"].str.replace("'", "''")
df["name"] = df["name"].str.replace("'", "''")

fmt = "({location}, {location_type}, '{state}', {area}, {latitude}, {longitude}, '{fullname}', '{name}')"

inserts = ", ".join(fmt.format(**x) for _, x in df.iterrows())

output = (
    """
INSERT INTO meta.locations (location, location_type, state, area, latitude, longitude, fullname, name)
VALUES
"""
    + " "
    + inserts
    + ";"
)

print(output)
