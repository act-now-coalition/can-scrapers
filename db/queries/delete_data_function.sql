DROP FUNCTION IF EXISTS delete_scraped_data (text, text, int, date, date, int, text, text, text);

CREATE OR REPLACE FUNCTION delete_scraped_data (_provider text = 'state', _variable_regex text = '.+', _state_fips int = NULL, _start_date date = NULL, _end_date date = NULL, _location int = NULL, _location_type_regex text = '.+', _unit_regex text = '.+', _measurement_regex text = '.+', out rows_changed int, out delete_batch_id bigint)
LANGUAGE plpgsql
AS $$
DECLARE
    myquery text;
    provider_where text := format('where provider_id in (select id from meta.covid_providers where name ~ %L)', _provider);
    location_id_and text := CASE WHEN _location IS NOT NULL THEN
        format('and location_id in (select id from meta.locations where location = %s)', _location)
    WHEN _state_fips IS NOT NULL THEN
        format('and location_id in(select id from meta.locations where state_fips = %s and location_type ~ %L', _state_fips, _location_type_regex)
    ELSE
        ''
    END;
    start_date_and text := CASE WHEN _start_date IS NOT NULL THEN
        format('and dt >= %L::DATE', _start_date)
    ELSE
        ''
    END;
    end_date_and text := CASE WHEN _end_date IS NOT NULL THEN
        format('and dt <= %L::DATE', _end_date)
    ELSE
        ''
    END;
    variables_and text := format('and variable_id in (select id from meta.covid_variables where category ~ %L and unit ~ %L and measurement ~ %L)', _variable_regex, _unit_regex, _measurement_regex);
BEGIN
    SELECT
        nextval('data.delete_batch_id_seq') INTO delete_batch_id;
    myquery := format('update data.covid_observations
      set deleted = TRUE, delete_batch_id = %s
          %s
          %s
          %s
          %s
          %s;
      ', delete_batch_id, provider_where, location_id_and, start_date_and, end_date_and, variables_and);
    RAISE NOTICE 'query: %', myquery;
    EXECUTE myquery;
    get diagnostics rows_changed = row_count;
END;
$$
