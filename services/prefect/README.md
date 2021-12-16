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
mount ~/scraper-outputs
cd can-scrapers/services/prefect
make sync_services
make start_services
conda activate prefect-can-scrapers
prefect server create-tenant --name can --slug can
prefect create project can-scrape
make setup_nginx
sudo certbot --nginx
```


**Setup Flows**:

```shell
conda activate prefect-can-scrapers
cd ~/can-scrapers/services/prefect/flows
python generated_flows.py
python clean_sql.py
python update_api_view.py
```

**Setup webhook**:

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
    - Run “sudo su sglyon” to gain access to his account and navigate to /home/sglyon
    - Remount the storage bucket by running “mount scraper-outputs”
    - Restart the Prefect infrastructure by running “make restart_services”


### Troubleshooting

If gcsfuse is killed it might be necessary to remount the file system. Symptoms of this can include:

* Scrapers failing on the `create_scraper` task with the message, 
  >`[Errno 107] Transport endpoint is not connected: '/home/sglyon/scraper-outputs.`
* The Prefect console reading a message like, `Jun 21 10:14:55 prefect kernel ...  Killed process ... (gcsfuse)`

The steps to fix this are to:
* Follow the steps above to SSH into the VM and navigate to /home/sglyon,
* Un-mount the current filesystem using `fusermount -u scraper-outputs`
* If the above fails with the error `fusermount: failed to unmount /home/sglyon/scraper-outputs: Device or resource busy`, un-mount the filesystem using `sudo umount -l scraper-outputs`
* Re-mount the filesystem using `mount ~/scraper-outputs`. This runs gcsfuse and links the ~/scraper-outputs directory to the cloud storage bucket
* Check the Prefect server dashboard to ensure that the flows have commenced. 
