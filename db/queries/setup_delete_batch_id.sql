ALTER TABLE data.covid_observations
    ADD COLUMN delete_batch_id bigint;

CREATE INDEX "covid_observations_delete_batch_id" ON "data"."covid_observations" USING BTREE ("delete_batch_id");

CREATE SEQUENCE data.delete_batch_id_seq
    START 1;

