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
- Ran the setup script: `bash services/prefect/setup_gcp_instance.sh` (it might be necessary to instead run the commands in this script one by one)
- Reboot the instance (will log you out -- wait a minute and ssh in again): `sudo reboot`

**Launch steps**:

Generate an API key for Prefect Cloud at `https://app.prefect.cloud/my/profile`.
We'll use this to login to Prefect cloud in the CLI.  

```shell
mount ~/scraper-outputs
cd can-scrapers/services/prefect
make sync_services
make start_services
conda activate prefect-can-scrapers
prefect prefect cloud login -k <API_KEY>
```

**Setup Flows**:

```shell
conda activate prefect-can-scrapers
cd ~/can-scrapers
python -m services.prefect.deployment deploy-flows -c all
```

**Set Prefect secrets**:

Configure Prefect Blocks that hold secrets, including the database connection and Github access token.
These secrets can be found in 1password.

Go to `https://app.prefect.cloud/` > Blocks > Add new block (the +).

Add secrets

- `covid-db-conn-uri` with the connection string for the Postgres database
- `github-action-pat` with a Github Personal Access Token with repo access to `can-scrapers`

**Setup webhook (Currently deprecated as of 06/26/2023)**:

- Generate a random password
- Add Change the field `[0].trigger-rule.and[0].match.secret` from `PASSWORD` to your random password
- Start the webhook server...

```shell
cd ~/can-scrapers/services/webhook
make setup_nginx
make sync
make start
```

- Create a webhook on the github repo:
  - Will need to set the payload URL to https://SERVER/webhook/pull-can-scrapers
  - Keep content type as `applicatoin/x-www-form-urlencoded`
  - Set the Secret field to the password you created above
  - keep SSL verification
  - Only send `push` event
  - Make sure it is active
  - Submit


Now any time a push is made to the master branch of the repo, a push event will be sent to the server and the webhook will cause a git pull

**Maintenance**:

### Restarting the Prefect Infrastructure

- Navigate to the Prefect virtual machine (VM),
  - Go to the covidactnow-dev Google Cloud Console
  - Under the “Resources” section, select “Compute Engine”
  - From the list, open the “prefect” machine
- Restart the VM,
  - Restart the VM by clicking the “Reset” button, and clicking “Reset” in the dialogue box that appears. This effectively reboots the machine. 
- SSH into the Prefect virtual machine:
  - Click the “SSH” button then select “continue” to open a connection to the VM. 
- Mount the “scraper-outputs” storage bucket and restart the Prefect server and agent. 
  - To do these next steps, we will need to access Spencer’s account:
    - Run `sudo su sean` to gain access to his account and navigate to `/home/sean`
    - Remount the storage bucket by running `mount scraper-outputs`
    - Restart the Prefect infrastructure by running `make restart_services`


### Troubleshooting

#### Restarting GCSFuse

If gcsfuse is killed it might be necessary to remount the file system. Symptoms of this can include:

* Scrapers failing on the `create_scraper` task with the message, 
  >`[Errno 107] Transport endpoint is not connected: '/home/sean/scraper-outputs.`
* The Prefect console reading a message like, `Jun 21 10:14:55 prefect kernel ...  Killed process ... (gcsfuse)`

The steps to fix this are to:
* Follow the steps above to SSH into the VM and navigate to /home/sean,
* Un-mount the current filesystem using `fusermount -u scraper-outputs`
* If the above fails with the error `fusermount: failed to unmount /home/sean/scraper-outputs: Device or resource busy`, un-mount the filesystem using `sudo umount -l scraper-outputs`
* Re-mount the filesystem using `mount ~/scraper-outputs`. This runs gcsfuse and links the ~/scraper-outputs directory to the cloud storage bucket
* Check the Prefect server dashboard to ensure that the flows have commenced. 

#### Restarting Prefect Agent Service

If flows are being scheduled but not executed, it's possible that the Prefect Agent service has broken.
To check the status and restart the service, follow the steps:

```bash
# SSH into `prefect` VM in `covidactnow-dev` google cloud console

# Restart prefect agent service
sudo su sean  # if prompted, password is `password1`
sudo systemctl restart prefect-agent

# Check status with 
sudo systemctl status prefect-agent
```
