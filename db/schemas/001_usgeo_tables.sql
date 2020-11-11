DROP TABLE IF EXISTS meta.location CASCADE;
DROP TABLE IF EXISTS meta.location_type CASCADE;


CREATE TABLE meta.location_type (
    id SERIAL PRIMARY KEY,
    name TEXT UNIQUE
);

COMMENT ON TABLE meta.location_type IS E'This table contains information on type of geography that is being stored.

It takes values

* `county`
* `state`
* `cbsa`
* `hospital_region`

Change log
----------
* 2020-10-27: Created `meta.location_type` and filled with the first four initial values `county`, `state`, `metro`, `hospital_region`
';

INSERT INTO meta.location_type (name)
   VALUES ('county'),
          ('state'),
          ('cbsa'),
          ('hospital_region');


CREATE TABLE meta.locations (
  id SERIAL PRIMARY KEY,
  location BIGINT,
  location_type SMALLINT REFERENCES meta.location_type (id),
  state text,
  name text,
  area real,
  latitude real,
  longitude real,
  fullname text
);
CREATE UNIQUE INDEX loc_ind ON meta.locations (location);

COMMENT ON TABLE meta.locations IS E'This table contains information about various geographies and contains `location` which is a unique identifier for each geography that we track.

The `location` values is generated following the following criterion:

* If the geography is a state or county then it is assigned the corresponding fips code
  - A state fips code is described by numbers between 0 and 99 (i.e. the number 6 corresponds to CA)
  - A county fips code is described by 5 digits -- The first two correspond to the state that the county is in and the remaining 3 are a county fips code that is unique only within the state (i.e. 6037 corresponds to Los Angeles County in CA)
* If the geography is a core based statistical area (metropolitan/micropolitan statistical area) then we use the 5 digit CBSA code from the census
* If the geography is a hospital region, we use our own internal convention with a 8 digit number. The first 2 digits correspond to the state fips code, the next 3 digits are set to 9, and the final 3 digits denote which hospital region in the specified state

Change log
----------
* 2020-10-28: Created `meta.locations` and established `location` conventions for `state`, `county`, `cbsa`, and `hospital_region`
';

