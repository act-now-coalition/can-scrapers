DROP SCHEMA IF EXISTS meta CASCADE;

DROP SCHEMA IF EXISTS api CASCADE;

DROP SCHEMA IF EXISTS data CASCADE;

CREATE SCHEMA meta;

CREATE SCHEMA api;

CREATE SCHEMA data;

CREATE ROLE postgrest WITH
  LOGIN
  NOSUPERUSER
  INHERIT
  NOCREATEDB
  NOCREATEROLE
  NOREPLICATION;

CREATE ROLE covid_anon WITH
  NOLOGIN
  NOSUPERUSER
  INHERIT
  NOCREATEDB
  NOCREATEROLE
  NOREPLICATION;


DO
  $do$
  BEGIN
    IF NOT EXISTS (
      SELECT FROM pg_catalog.pg_roles  -- SELECT list can be empty for this
       WHERE  rolname = 'pguser') THEN

      CREATE ROLE pguser LOGIN PASSWORD 'password';
   END IF;
END
$do$;

grant covid_anon to pguser;
GRANT usage on schema api to covid_anon;
grant usage on schema meta to covid_anon;
ALTER default PRIVILEGES in schema api grant select on tables to covid_anon;
ALTER default PRIVILEGES in schema meta grant select on tables to covid_anon;
