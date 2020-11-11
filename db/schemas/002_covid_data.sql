DROP TABLE IF EXISTS meta.covid_variables;
DROP TABLE IF EXISTS data.covid_observations;
DROP TABLE IF EXISTS data.covid_providers;
DROP TABLE IF EXISTS meta.covid_categories;
DROP TABLE IF EXISTS meta.covid_measurement;
DROP TABLE IF EXISTS meta.covid_unit;
DROP TABLE IF EXISTS meta.covid_demographics;


CREATE TABLE meta.covid_categories
(
    category TEXT,
    subcategory TEXT UNIQUE,
    PRIMARY KEY (category, subcategory)
);

COMMENT ON TABLE meta.covid_categories IS E'This table contains categories and sub-categories of COVID variables.

The categories include:
* `case`
* `death`
* `hospital`
* `test`

The sub-categories currently covered (associated their their category) are:

* `cases` (`case`)
* `cases_probable` (`case`)
* `cases_confirmed` (`case`)
* `deaths` (`death`)
* `deaths_probable` (`death`)
* `deaths_confirmed` (`death`)
* `hospital_beds_available` (`hospital`)
* `hospital_beds_capacity` (`hospital`)
* `hospital_beds_in_use` (`hospital`)
* `hospital_beds_in_use_covid` (`hospital`)
* `icu_beds_available` (`hospital`)
* `icu_beds_capacity` (`hospital`)
* `icu_beds_in_use` (`hospital`)
* `icu_beds_in_use_covid` (`hospital`)
* `ventilators_available` (`hospital`)
* `ventilators_capacity` (`hospital`)
* `ventilators_in_use` (`hospital`)
* `ventilators_in_use_covid` (`hospital`)
* `antibody_tests_negative` (`test`)
* `antibody_tests_positive` (`test`)
* `antibody_tests_total` (`test`)
* `antigen_tests_negative` (`test`)
* `antigen_tests_positive` (`test`)
* `antigen_tests_total` (`test`)
* `antigen_pcr_tests_negative` (`test`)
* `antigen_pcr_tests_positive` (`test`)
* `antigen_pcr_tests_total` (`test`)
* `pcr_tests_negative` (`test`)
* `pcr_tests_positive` (`test`)
* `pcr_tests_total` (`test`)
* `unspecified_tests_negative` (`test`)
* `unspecified_tests_positive` (`test`)
* `unspecified_tests_total` (`test`)
* `reported_positivity` (`test`)

Change log
----------
 * 2020-10-27: Created `meta.covid_categories` and filled with the first initial variables.
 * 2020-11-09: Created new values for adult and pediatric beds
';

INSERT INTO meta.covid_categories (category, subcategory)
VALUES ('case', 'cases'),
       ('case', 'cases_probable'),
       ('case', 'cases_confirmed'),
       ('death', 'deaths'),
       ('death', 'deaths_probable'),
       ('death', 'deaths_confirmed'),

       ('hospital', 'adult_hospital_beds_available'),
       ('hospital', 'adult_hospital_beds_capacity'),
       ('hospital', 'adult_hospital_beds_in_use'),
       ('hospital', 'adult_hospital_beds_in_use_covid'),
       ('hospital', 'pediatric_hospital_beds_available'),
       ('hospital', 'pediatric_hospital_beds_capacity'),
       ('hospital', 'pediatric_hospital_beds_in_use'),
       ('hospital', 'pediatric_hospital_beds_in_use_covid'),
       ('hospital', 'hospital_beds_available'),
       ('hospital', 'hospital_beds_capacity'),
       ('hospital', 'hospital_beds_in_use'),
       ('hospital', 'hospital_beds_in_use_covid'),

       ('hospital', 'adult_icu_beds_available'),
       ('hospital', 'adult_icu_beds_capacity'),
       ('hospital', 'adult_icu_beds_in_use'),
       ('hospital', 'adult_icu_beds_in_use_covid'),
       ('hospital', 'pediatric_icu_beds_available'),
       ('hospital', 'pediatric_icu_beds_capacity'),
       ('hospital', 'pediatric_icu_beds_in_use'),
       ('hospital', 'pediatric_icu_beds_in_use_covid'),
       ('hospital', 'icu_beds_available'),
       ('hospital', 'icu_beds_capacity'),
       ('hospital', 'icu_beds_in_use'),
       ('hospital', 'icu_beds_in_use_covid'),

       ('hospital', 'ventilators_available'),
       ('hospital', 'ventilators_capacity'),
       ('hospital', 'ventilators_in_use'),
       ('hospital', 'ventilators_in_use_covid'),

       ('test', 'antibody_tests_negative'),
       ('test', 'antibody_tests_positive'),
       ('test', 'antibody_tests_total'),
       ('test', 'antigen_tests_negative'),
       ('test', 'antigen_tests_positive'),
       ('test', 'antigen_tests_total'),
       ('test', 'antigen_pcr_tests_negative'),
       ('test', 'antigen_pcr_tests_positive'),
       ('test', 'antigen_pcr_tests_total'),
       ('test', 'pcr_tests_negative'),
       ('test', 'pcr_tests_positive'),
       ('test', 'pcr_tests_total'),
       ('test', 'unspecified_tests_negative'),
       ('test', 'unspecified_tests_positive'),
       ('test', 'unspecified_tests_total'),
       ('test', 'reported_positivity');


CREATE TABLE meta.covid_measurement
(
    name TEXT UNIQUE PRIMARY KEY
);

COMMENT ON TABLE meta.covid_measurement IS E'This table contains the different types of classifications variables might fall into. The main use is to identify whether the variables are "new daily" or "cumulative" values.

Change log
----------
* 2020-10-27: Created `meta.covid_measurement` and added the "cumulative" and "new" values
* 2020-11-05: Created the "current" value
';

INSERT INTO meta.covid_measurement (name)
VALUES ('cumulative'),
       ('new'),
       ('current');


CREATE TABLE meta.covid_unit
(
    name TEXT UNIQUE PRIMARY KEY
);

COMMENT ON TABLE meta.covid_unit IS E'This table contains the different forms of measurement that data can be reported in.

Data reported can be reported as:
* `people`: Number of people for the particular variable.
* `percentage`: Used for when values are reported as a percent of a whole
* `specimens`: The number of unique specimens (used for testing)
* `test_encounters`: The number of unique people per day (used for testing)
* `unique_people`: The number of unique people (used for testing)
* `unknown`: Unclear what is being tracked
* `beds`: Number of beds (context of a hospital)

In the future, if states began to report why people were tested, we could add additional entries to this table that
could be used to track why people were tested -- For example, we might call one `test_encounters_symptomatic` for
people who were tested because they were symptomatic.

Change log
----------
* 2020-10-27: Created `meta.covid_unit` and added the "people", "percentage", "specimens", "test_encounters", "unique_people", and "unknown"
* 2020-11-09: Created new values for "beds"
';

INSERT INTO meta.covid_unit (name)
VALUES ('people'),
       ('percentage'),
       ('specimens'),
       ('test_encounters'),
       ('unique_people'),
       ('unknown'),
       ('beds');



CREATE TABLE meta.covid_variables
(
    id SERIAL PRIMARY KEY,
    category TEXT REFERENCES meta.covid_categories (subcategory),
    measurement TEXT REFERENCES meta.covid_measurement (name),
    unit TEXT REFERENCES meta.covid_unit (name)
);

INSERT INTO meta.covid_variables (category, measurement, unit)
VALUES ('cases', 'cumulative', 'people'),
       ('cases', 'new', 'people'),
       ('deaths', 'cumulative', 'people'),
       ('deaths', 'new', 'people'),

       ('hospital_beds_in_use_covid', 'cumulative', 'beds'),
       ('hospital_beds_in_use_covid', 'new', 'beds'),

       ('hospital_beds_in_use_covid', 'current', 'beds'),
       ('hospital_beds_in_use', 'current', 'beds'),
       ('hospital_beds_available', 'current', 'beds'),
       ('hospital_beds_capacity', 'current', 'beds'),

       ('adult_icu_beds_in_use', 'current', 'beds'),
       ('adult_icu_beds_in_use_covid', 'current', 'beds'),
       ('adult_icu_beds_available', 'current', 'beds'),
       ('adult_icu_beds_capacity', 'current', 'beds'),
       ('pediatric_icu_beds_in_use', 'current', 'beds'),
       ('pediatric_icu_beds_in_use_covid', 'current', 'beds'),
       ('pediatric_icu_beds_available', 'current', 'beds'),
       ('pediatric_icu_beds_capacity', 'current', 'beds'),
       ('icu_beds_in_use_covid', 'current', 'beds'),
       ('icu_beds_in_use', 'current', 'beds'),
       ('icu_beds_available', 'current', 'beds'),
       ('icu_beds_capacity', 'current', 'beds'),

       ('antibody_tests_total', 'cumulative', 'specimens'),
       ('antibody_tests_negative', 'cumulative', 'specimens'),
       ('antibody_tests_positive', 'cumulative', 'specimens'),
       ('antigen_tests_total', 'cumulative', 'specimens'),
       ('antigen_tests_negative', 'cumulative', 'specimens'),
       ('antigen_tests_positive', 'cumulative', 'specimens'),
       ('pcr_tests_total', 'cumulative', 'specimens'),
       ('pcr_tests_negative', 'cumulative', 'specimens'),
       ('pcr_tests_positive', 'cumulative', 'specimens'),
       ('unspecified_tests_total', 'cumulative', 'specimens'),
       ('unspecified_tests_negative', 'cumulative', 'specimens'),
       ('unspecified_tests_positive', 'cumulative', 'specimens'),

       ('antibody_tests_total', 'new', 'specimens'),
       ('antibody_tests_negative', 'new', 'specimens'),
       ('antibody_tests_positive', 'new', 'specimens'),
       ('antigen_tests_total', 'new', 'specimens'),
       ('antigen_tests_negative', 'new', 'specimens'),
       ('antigen_tests_positive', 'new', 'specimens'),
       ('pcr_tests_total', 'new', 'specimens'),
       ('pcr_tests_negative', 'new', 'specimens'),
       ('pcr_tests_positive', 'new', 'specimens'),
       ('unspecified_tests_total', 'new', 'specimens'),
       ('unspecified_tests_negative', 'new', 'specimens'),
       ('unspecified_tests_positive', 'new', 'specimens'),

       ('antibody_tests_total', 'cumulative', 'test_encounters'),
       ('antibody_tests_negative', 'cumulative', 'test_encounters'),
       ('antibody_tests_positive', 'cumulative', 'test_encounters'),
       ('antigen_tests_total', 'cumulative', 'test_encounters'),
       ('antigen_tests_negative', 'cumulative', 'test_encounters'),
       ('antigen_tests_positive', 'cumulative', 'test_encounters'),
       ('pcr_tests_total', 'cumulative', 'test_encounters'),
       ('pcr_tests_negative', 'cumulative', 'test_encounters'),
       ('pcr_tests_positive', 'cumulative', 'test_encounters'),
       ('unspecified_tests_total', 'cumulative', 'test_encounters'),
       ('unspecified_tests_negative', 'cumulative', 'test_encounters'),
       ('unspecified_tests_positive', 'cumulative', 'test_encounters'),

       ('antibody_tests_total', 'new', 'test_encounters'),
       ('antibody_tests_negative', 'new', 'test_encounters'),
       ('antibody_tests_positive', 'new', 'test_encounters'),
       ('antigen_tests_total', 'new', 'test_encounters'),
       ('antigen_tests_negative', 'new', 'test_encounters'),
       ('antigen_tests_positive', 'new', 'test_encounters'),
       ('pcr_tests_total', 'new', 'test_encounters'),
       ('pcr_tests_negative', 'new', 'test_encounters'),
       ('pcr_tests_positive', 'new', 'test_encounters'),
       ('unspecified_tests_total', 'new', 'test_encounters'),
       ('unspecified_tests_negative', 'new', 'test_encounters'),
       ('unspecified_tests_positive', 'new', 'test_encounters'),

       ('antibody_tests_total', 'cumulative', 'unique_people'),
       ('antibody_tests_negative', 'cumulative', 'unique_people'),
       ('antibody_tests_positive', 'cumulative', 'unique_people'),
       ('antigen_tests_total', 'cumulative', 'unique_people'),
       ('antigen_tests_negative', 'cumulative', 'unique_people'),
       ('antigen_tests_positive', 'cumulative', 'unique_people'),
       ('pcr_tests_total', 'cumulative', 'unique_people'),
       ('pcr_tests_negative', 'cumulative', 'unique_people'),
       ('pcr_tests_positive', 'cumulative', 'unique_people'),
       ('unspecified_tests_total', 'cumulative', 'unique_people'),
       ('unspecified_tests_negative', 'cumulative', 'unique_people'),
       ('unspecified_tests_positive', 'cumulative', 'unique_people'),

       ('antibody_tests_total', 'new', 'unique_people'),
       ('antibody_tests_negative', 'new', 'unique_people'),
       ('antibody_tests_positive', 'new', 'unique_people'),
       ('antigen_tests_total', 'new', 'unique_people'),
       ('antigen_tests_negative', 'new', 'unique_people'),
       ('antigen_tests_positive', 'new', 'unique_people'),
       ('pcr_tests_total', 'new', 'unique_people'),
       ('pcr_tests_negative', 'new', 'unique_people'),
       ('pcr_tests_positive', 'new', 'unique_people'),
       ('unspecified_tests_total', 'new', 'unique_people'),
       ('unspecified_tests_negative', 'new', 'unique_people'),
       ('unspecified_tests_positive', 'new', 'unique_people'),

       ('antibody_tests_total', 'cumulative', 'unknown'),
       ('antibody_tests_negative', 'cumulative', 'unknown'),
       ('antibody_tests_positive', 'cumulative', 'unknown'),
       ('antigen_tests_total', 'cumulative', 'unknown'),
       ('antigen_tests_negative', 'cumulative', 'unknown'),
       ('antigen_tests_positive', 'cumulative', 'unknown'),
       ('pcr_tests_total', 'cumulative', 'unknown'),
       ('pcr_tests_negative', 'cumulative', 'unknown'),
       ('pcr_tests_positive', 'cumulative', 'unknown'),
       ('unspecified_tests_total', 'cumulative', 'unknown'),
       ('unspecified_tests_negative', 'cumulative', 'unknown'),
       ('unspecified_tests_positive', 'cumulative', 'unknown'),

       ('antibody_tests_total', 'new', 'unknown'),
       ('antibody_tests_negative', 'new', 'unknown'),
       ('antibody_tests_positive', 'new', 'unknown'),
       ('antigen_tests_total', 'new', 'unknown'),
       ('antigen_tests_negative', 'new', 'unknown'),
       ('antigen_tests_positive', 'new', 'unknown'),
       ('pcr_tests_total', 'new', 'unknown'),
       ('pcr_tests_negative', 'new', 'unknown'),
       ('pcr_tests_positive', 'new', 'unknown'),
       ('unspecified_tests_total', 'new', 'unknown'),
       ('unspecified_tests_negative', 'new', 'unknown'),
       ('unspecified_tests_positive', 'new', 'unknown');


CREATE TABLE meta.covid_demographics
(
    id SERIAL PRIMARY KEY,
    age TEXT,
    race TEXT,
    sex TEXT
);

COMMENT ON TABLE meta.covid_demographics IS E'This table contains demographic categories that variables can belong to.

The demographics tracked currently include:
* `all`: Not targeting a specific demographic
';

INSERT INTO meta.covid_demographics (age, race, sex)
VALUES ('all', 'all', 'all');


CREATE TABLE data.covid_providers
(
    id SERIAL PRIMARY KEY,
    name TEXT UNIQUE,
    priority INT
);


INSERT INTO data.covid_providers (name, priority)
VALUES ('county', 1000),
       ('state', 2000),
       ('usafacts', 3000),
       ('ctp', 4000),
       ('hhs', 5000)
;


CREATE TABLE data.covid_observations
(
    vintage TIMESTAMP,
    dt DATE,
    location BIGINT REFERENCES meta.locations (location),
    variable_id SMALLINT REFERENCES meta.covid_variables (id),
    demographic_id SMALLINT REFERENCES meta.covid_demographics (id),
    value REAL,
    provider INT REFERENCES data.covid_providers (id) NOT NULL,
    PRIMARY KEY (vintage, dt, location, variable_id, demographic_id)
);

COMMENT ON TABLE data.covid_observations IS E'This table contains all of the collected data from various COVID sources.';

COMMENT ON COLUMN data.covid_observations.vintage IS E'When the data was accessed UTC time (at an hour frequency)';
COMMENT ON COLUMN data.covid_observations.dt IS E'The date for which the data corresponds';
COMMENT ON COLUMN data.covid_observations.location IS E'The location code identifying the geography. See `meta.locations` for more information.';
COMMENT ON COLUMN data.covid_observations.variable_id IS E'The id of the variable observed. See `meta.covid_variables` for more information.';
COMMENT ON COLUMN data.covid_observations.demographic_id IS E'The id of the demographic being observed. See `meta.covid_demographics` for more information.';
COMMENT ON COLUMN data.covid_observations.value IS E'The value of the variable for the given location, on a date, and a vintage';
COMMENT ON COLUMN data.covid_observations.provider IS E'The type of data source that the data came from';
