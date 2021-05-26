GRANT usage ON SCHEMA meta TO can_data_eng;

GRANT usage ON SCHEMA data TO can_data_eng;

GRANT SELECT ON ALL tables IN SCHEMA data TO can_data_eng;

GRANT SELECT ON ALL tables IN SCHEMA public TO can_data_eng;

GRANT SELECT ON ALL tables IN SCHEMA meta TO can_data_eng;

GRANT UPDATE ON data.covid_observations TO can_data_eng;

GRANT EXECUTE ON FUNCTION public.select_scraped_data TO can_data_eng;

GRANT EXECUTE ON FUNCTION public.delete_scraped_data TO can_data_eng;

GRANT EXECUTE ON FUNCTION public.undelete_by_delete_batch_id TO can_data_eng;

GRANT UPDATE ON SEQUENCE data.delete_batch_id_seq
    TO can_data_eng;

