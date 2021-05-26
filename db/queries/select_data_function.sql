DROP FUNCTION IF EXISTS select_scraped_data (int, text, text, int, date, date, int, text, text, text);

CREATE OR REPLACE FUNCTION public.delete_scraped_data(_provider text DEFAULT 'state'::text, _variable_regex text DEFAULT '.+'::text, _state_fips integer DEFAULT NULL::integer, _start_date date DEFAULT NULL::date, _end_date date DEFAULT NULL::date, _location integer DEFAULT NULL::integer, _location_type_regex text DEFAULT '.+'::text, _unit_regex text DEFAULT '.+'::text, _measurement_regex text DEFAULT '.+'::text, OUT rows_changed integer, OUT delete_batch_id bigint)
 RETURNS record
 LANGUAGE plpgsql
AS $function$
DECLARE
    myquery text;
    provider_where text := format('where provider_id in (select id from meta.covid_providers where name ~ %L)', _provider);
    location_id_and text := CASE WHEN _location IS NOT NULL THEN
        format('and location_id in (select id from meta.locations where location = %s)', _location)
    WHEN _state_fips IS NOT NULL THEN
        format('and location_id in(select id from meta.locations where state_fips = %s and location_type ~ %L)', _state_fips, _location_type_regex)
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
          %s
      ', delete_batch_id, provider_where, location_id_and, start_date_and, end_date_and, variables_and);
    RAISE NOTICE 'query: %', myquery;
    EXECUTE myquery;
    get diagnostics rows_changed = row_count;
END;
$function$
