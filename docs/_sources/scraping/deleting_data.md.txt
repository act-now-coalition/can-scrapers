# Deleting Data

From time to time some incorrect data makes it into the database

This eventually ends up in the parquet file and then on the CAN website and into the CAN API

We have a system for deleting data

The system is documented on this page

## SQL in Production

Right now our strategy for deleting bad data is to write SQL against our production database

To make this process easier and less scary, we have written three SQL functions that should be used to handle deleting records

### `select_scraped_data`

The first SQL function is called `select_scraped_data` and can be called as follows:

```sql
select * from select_scraped_data(...args)
```

The `...args` is a placeholder for optional arguments that can be used to filter/narrow down the returned results.

Each optional argument should be given in format `name => value`.

The optional argument are:

- `_limit`(int default = 5000): The maximum number of rows to be returned by the query
- `_provider`(textdefault = 'state'): The type of provider. Should one of the items in `can_tools/bootstrap_data/covid_providers.csv`
- `_variable_regex`(TEXT default = '.+'): A regular expression for filtering on the name of the variable (applied to the `category` column from `can_tools/bootstrap_data/covid_variables.csv`)
- `_state_fips`(int default = NULL): An integer representing the state fips code
- `_location`(int default = NULL): A single fips code -- if this is given `_state_fips` is ignored
- `_start_date`(date default = NULL): A starting date for data
- `_end_date`(date default = NULL): An ending date for data
- `_location_type_regex`(text default = '.+'): A location type. One of `nation`, `state`, or `county`
- `_unit_regex`(text default = '.+'): A regular expression used to filter on the `unit` column (applied to the `unit` column from `can_tools/bootstrap_data/covid_variables.csv`)
- `_measurement_regex`(text default = '.+'): A regular expression used to filter on the `measurement` column (applied to the `measurement` column from `can_tools/bootstrap_data/covid_variables.csv`)

An example of how this can be used is given below:

```sql
SELECT
    *
FROM
    select_scraped_data (_provider => 'state',
        _variable_regex => '.*vaccine.*',
        _start_date => '2021-05-20',
        _end_date => '2021-05-24',
        _unit_regex => 'doses',
        _location_type_regex => 'county',
        _measurement_regex => 'cumulative',
        _state_fips => 12);
```

This would return data between May 20, 2021 and May 24, 2021 (inclusive) for counties in Florida where the "vaccine" is part of the variable name and unit and measurement are cumulative doses. This data would also have been scraped from the Florida state dashboard

### `delete_scraped_data`

The second helper function is called `delete_scraped_data`

It takes all the same optional arguments as `select_scraped_data`

when this routine is called two things happen:

1. All rows that would be returned by the equivalent call to `select_scraped_data` are updated so that the `deleted` column is set to `TRUE` AND the `delete_batch_id` is set to a constant integer representing this call to the `delete_scraped_data` function
2. Two integers are returned representing first the number of rows changed and second the `delete_batch_id`

Only rows where `deleted = FALSE` makes it into the `api.covid_us` table, and therefore into the parquet file

The purpose of the `delete_batch_id` column is to make it easier to undo an incorrect change.

After using `select_scraped_data` to identify rows that should be deleted, simply change the function name to `delete_scraped_data` to do the deletion

Continuing the example from above, to delete that Florida data we would execute

```sql
SELECT
    *
FROM
    delete_scraped_data(_provider => 'state',
        _variable_regex => '.*vaccine.*',
        _start_date => '2021-05-20',
        _end_date => '2021-05-24',
        _unit_regex => 'doses',
        _location_type_regex => 'county',
        _measurement_regex => 'cumulative',
        _state_fips => 12);
```

### `undelete_by_delete_batch_id`

The final helper function we have written makes it easy to undo a delete operation

`undelete_by_delete_batch_id` accepts a single argument, an integer for the `delete_batch_id`

When this function is called, all rows of data will have `deleted` set to `FALSE` and the `delete_batch_id` will be set back to `NULL`

Suppose we were given a `delete_batch_id` of 42 when calling the `delete_scraped_data` in our Florida example above...

To undo this deletion we need to call

```sql
SELECT
    *
FROM
    undelete_by_delete_batch_id(42);
```

This will return a single row and column letting us know how many rows were updated

## Timeseries scrapers

Note that when a scraper returns a timeseries, some deleted data may have the `value` and `last_updated` columns update

When this happens we will also mark `deleted` as FALSE and set `delete_batch_id` to NULL

In effect, we make the assumption that if a scraper is trying to insert a data point, it should not begin as deleted
