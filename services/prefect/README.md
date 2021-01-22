# Prefect

Can-scrapers setup for prefect

## Local

TODO

## Setting up gcp instance

**Pre-req**:

- Create a service account with IAM role storage admin
  - I also added condition for just one bucket
  - Service account email address was "can-scrape-storage-accessor@covidactnow-dev.iam.gserviceaccount.com", which is why this appears in the terraform entry for the `service_account.email` for the instance
**Setup steps**:

- Created instance in ../infrastructure/main.tf
- ssh'd into the instance
- Created a new ssh key to use as deploy key for repo: `ssh-keygen -t ed25519 -C "spencer@covidactnow.org"`
- Copied `~/.ssh/id_ed25519.pub` to a deploy key on the repo [here](https://github.com/covid-projections/can-scrapers/settings/keys/new)
- Cloned the repo: `git clone git@github.com:covid-projections/can-scrapers.git`
- Cd into the repo: `cd can-scrapers`
- Edited the file `services/prefect/prefect-agent.service` and add a line `Environment="COVID_DB_CONN_URI=postgresql://pguser:PASSWORD@35.245.78.43:5432/covid"` where `PASSWORD` is replaced by the actual password for the postgres db
- Ran the setup script: `bash services/prefect/setup_gcp_instance.sh`
- Reboot the instance (will log you out -- wait a minute and ssh in again): `sudo reboot`

**Launch steps**:

```shell
cd can-scrapers/services/prefect
make sync_services
make start_services
conda activate prefect-can-scrapers
prefect server create-tenant --name can --slug can
prefect create project can-scrape
```

**Notes**:

Any time the server restarts, you should do `mount ~/scraper-outputs` to run gcsfuse and link the ~/scraper-outputs directory to the cloud storage bucket
