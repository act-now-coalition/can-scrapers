# docker
sudo apt update
sudo apt install apt-transport-https ca-certificates curl software-properties-common
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu focal stable"
sudo apt install docker-ce

# docker compose
sudo curl -L "https://github.com/docker/compose/releases/download/1.29.2/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

# miniconda
curl -LO http://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh
bash Miniconda3-latest-Linux-x86_64.sh -p ~/miniconda -b
rm Miniconda3-latest-Linux-x86_64.sh
echo "export PATH=${HOME}/miniconda/bin:${PATH}" >> ~/.bashrc
source ~/.bashrc
conda update -y conda
conda install -y -c conda-forge mamba
conda init bash
sudo su - $USER  # hack to apply conda settings

# create python environment
mamba create -n prefect-can-scrapers python=3.11
conda activate prefect-can-scrapers

sudo apt install gcc
pip install -r requirements.txt
pip install -e .

# install gcsfuse
sudo apt-get install fuse
export GCSFUSE_REPO=gcsfuse-`lsb_release -c -s`
echo "deb https://packages.cloud.google.com/apt $GCSFUSE_REPO main" | sudo tee /etc/apt/sources.list.d/gcsfuse.list
curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo apt-key add -
sudo apt-get update
sudo apt-get install gcsfuse
sudo groupadd fuse
sudo usermod -aG fuse $USER

# mount scraper bucket
mkdir ~/scraper-outputs
echo "can-scrape-outputs /home/sean/scraper-outputs gcsfuse rw,noauto,user" | sudo tee -a /etc/fstab > /dev/null
echo "export DATAPATH=/home/sean/scraper-outputs" | tee -a ~/.bashrc > /dev/null
mount ~/scraper-outputs

# misc
sudo apt-get install -y nginx
sudo snap install core
sudo snap refresh core
sudo snap install --classic certbot
sudo ln -s /snap/bin/certbot /usr/bin/certbot
