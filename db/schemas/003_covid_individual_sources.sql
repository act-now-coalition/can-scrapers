CREATE TABLE data.covid_official
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

COMMENT ON TABLE data.covid_official IS E'This table contains all of the collected data from official COVID sources.';


CREATE TABLE data.covid_usafacts
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

COMMENT ON TABLE data.covid_usafacts IS E'This table contains all of the collected COVID data from USAFacts.';
