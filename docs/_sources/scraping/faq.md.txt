# FAQ

This document contains a list of questions we've heard developers ask about how the system works, how to write a scraper, or anything else related to our data engineering efforts. Our intention is for this document to be updated frequently and be a living resource of common questions and their answers.

In code snippets below you will see references to a few variables (`d`, `engine`, `df`), these are

- `d`: an instance of a scraper
- `engine`: a sqlalchemy engine, most often the sqlite based dev engine
- `df`: a clean/normalized DataFrame that is the output of the `normalize` method

## Location_id sql error

*How to diagnose this problem:* when calling `d.put(engine, df)` you will see an error that looks like this:

```
IntegrityError: (sqlite3.IntegrityError) NOT NULL constraint failed: covid_observations.location_id
```

There are two possible cases for handling locations: using a `location_name` column with state or county name or using a `location` column with fips codes

### `location_name` column

If you have a `location_name` column, chances are you have a misspelled county name, a row that isn't a county (`All` or `Total` are common issues)


*How to fix this problem:* Try the following method: `d.find_unknown_location_id(engine, df)`

It will return rows of your DataFrame for which we do not recognize the county name

You can compare this list against the list of counties for that state, which you can obtain via:

```python
locs = pd.read_sql("select * from locations", engine)
state_locs = locs.loc[locs["state_fips"] == d.state_fips, :]
```

Most often, the fix in this situation is to fix spelling/capitalization for a county name (to match what is in `state_locs`) or delete the offending rows if they are obviously not counties

### `location` column

If instead you have a `location` column, check to make sure that each row of the `location` column maps into a known location for that state

You can use the `state_locs` DataFrame from the code snippet above to see all known locations for the state

## variable_id sql error

*How to diagnose this problem:* when calling `d.put(engine, df)` you will see an error that looks like this:

```
IntegrityError: (sqlite3.IntegrityError) NOT NULL constraint failed: covid_observations.variable_id
```

*How to fix this problem:* Try the following method: `d.find_unknown_location_id(engine, df)`

It will return rows of your DataFrame for which we do not recognize the variable (recall that a variable_id is defined by a triplet `("category", "measurement", "unit")` -- the CMU columns)

The most common fixes for this problem are:

- Fix spelling on one of the CMU columns
- Change recorded value of CMU columns to match a value in the file `can_tools/bootstrap_data/covid_variables.csv`
- If it is an entirely new type of variable, you may need to add a row to the `can_tools/bootstrap_data/covid_variables.csv` file and try to `.put` again
  - If you are adding a brand new value for any of category, measurement, unit you also need to add the correspoinding value to one of `can_tools/bootstrap_data/covid_{categories,measurements,units}.csv`

## demographic_id sql error

**TODO**
