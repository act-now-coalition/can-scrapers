DROP FUNCTION IF EXISTS select_scraped_data (int, text, text, int, date, date, int, text, text, text);

CREATE OR REPLACE FUNCTION select_scraped_data (_limit int = 5000, _provider text = 'state', _variable_regex text = '.+', _state_fips int = NULL, _start_date date = NULL, _end_date date = NULL, _location int = NULL, _location_type_regex text = '.+', _unit_regex text = '.+', _measurement_regex text = '.+')
    RETURNS TABLE (
        is_deleted boolean,
        delete_batch_id bigint,
        provider varchar,
        dt date,
        location_id varchar,
        LOCATION int8,
        location_type varchar,
        variable_name varchar,
        measurement varchar,
        unit varchar,
        age varchar,
        race varchar,
        ethnicity varchar,
        sex varchar,
        last_updated timestamp,
        source_url text,
        source_name text,
        value numeric)
    LANGUAGE plpgsql
    AS $$
DECLARE
    loc_where text := CASE WHEN _location IS NOT NULL THEN
        format('and location = %s', _location)
    WHEN _state_fips IS NOT NULL THEN
        format('and meta.locations.state_fips = %s', _state_fips)
    ELSE
        ''
    END;
    start_date_where text := CASE WHEN _start_date IS NOT NULL THEN
        format('and dt >= %L::DATE', _start_date)
    ELSE
        ''
    END;
    end_date_where text := CASE WHEN _end_date IS NOT NULL THEN
        format('and dt <= %L::DATE', _end_date)
    ELSE
        ''
    END;
    myquery text := format('select
     covid_observations.deleted as is_deleted,
     covid_observations.delete_batch_id,
  	covid_providers.name AS provider,
     covid_observations.dt,
     locations.id AS location_id,
     locations.location,
     locations.location_type,
     covid_variables.category AS variable_name,
     covid_variables.measurement,
     covid_variables.unit,
     covid_demographics.age,
     covid_demographics.race,
     covid_demographics.ethnicity,
     covid_demographics.sex,
     covid_observations.last_updated,
     covid_observations.source_url,
     covid_observations.source_name,
     covid_observations.value
    FROM data.covid_observations
      LEFT JOIN meta.covid_variables ON covid_variables.id = covid_observations.variable_id
      LEFT JOIN meta.locations ON locations.id::text = covid_observations.location_id::text
      LEFT JOIN meta.covid_providers ON covid_providers.id = covid_observations.provider_id
      LEFT JOIN meta.covid_demographics ON covid_demographics.id = covid_observations.demographic_id
      where meta.covid_providers.name ~ %L
      and meta.covid_variables.category ~ %L
      and location_type ~ %L
      and unit ~ %L
      and measurement ~ %L
      %s
      %s
      %s
      limit %L;', _provider, _variable_regex, _location_type_regex, _unit_regex, _measurement_regex, loc_where, start_date_where, end_date_where, _limit);
BEGIN
    RAISE NOTICE 'query: %', myquery;
    RETURN query EXECUTE myquery;
END;
$$
