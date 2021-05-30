DROP FUNCTION IF EXISTS undelete_by_delete_batch_id (int);

CREATE OR REPLACE FUNCTION undelete_by_delete_batch_id (_batch_id int, OUT rows_changed int)
LANGUAGE plpgsql
AS $$
DECLARE
    myquery text;
BEGIN
    myquery := format('update data.covid_observations
     set deleted = FALSE, delete_batch_id = NULL
     where delete_batch_id = %s;
     ', _batch_id);
    EXECUTE myquery;
    get diagnostics rows_changed = row_count;
END;
$$
