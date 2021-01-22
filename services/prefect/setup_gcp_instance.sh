# set up docker
sudo apt-get update

sudo apt-get install -y \
    apt-transport-https \
    ca-certificates \
    curl \
    gnupg-agent \
    software-properties-common \
    make \
    ffmpeg \
    libsm6 \
    libxext6

curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -

sudo apt-key fingerprint 0EBFCD88

sudo add-apt-repository \
   "deb [arch=amd64] https://download.docker.com/linux/ubuntu \
   $(lsb_release -cs) \
   stable"

sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io
sudo usermod -aG docker $USER
sudo su - $USER  # hack to activate docker group for current user

# set up docker compose
sudo curl -L "https://github.com/docker/compose/releases/download/1.28.0/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

# set up gh cli
sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-key C99B11DEB97541F0
sudo apt-add-repository https://cli.github.com/packages
sudo apt update
sudo apt install -y gh

# install miniconda
curl -LO http://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh
bash Miniconda3-latest-Linux-x86_64.sh -p ~/miniconda -b
rm Miniconda3-latest-Linux-x86_64.sh
echo "export PATH=${HOME}/miniconda/bin:${PATH}" >> ~/.bashrc
source ~/.bashrc
conda update -y conda
conda install -y -c conda-forge mamba
conda init bash
sudo su - $USER  # hack to apply conda settings

# set up conda env
mamba create -n prefect-can-scrapers python=3.7
conda activate prefect-can-scrapers

# install python deps
pip install -r requirements.txt
pip install -e .
pip install prefect pyarrow
prefect backend server

# set up gcsfuse for storing dag outputs
# install
export GCSFUSE_REPO=gcsfuse-`lsb_release -c -s`
echo "deb http://packages.cloud.google.com/apt $GCSFUSE_REPO main" | sudo tee /etc/apt/sources.list.d/gcsfuse.list
curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo apt-key add -
sudo apt-get update
sudo apt-get install -y gcsfuse
sudo usermod -a -G fuse $USER
sudo su - $USER  # hack to enable gcsfuse

# setup
mkdir ~/scraper-outputs
echo "can-scrape-outputs /home/sglyon/scraper-outputs gcsfuse rw,noauto,user" | sudo tee -a /etc/fstab > /dev/null
mount ~/scraper-outputs

# add environment variable for the scraper-outputs
echo "export DATAPATH=/home/sglyon/scraper-outputs" | tee -a ~/.bashrc > /dev/nul

# setup nginx for reverse proxy
sudo apt-get install -y nginx
sudo snap install core
sudo snap refresh core
sudo snap install --classic certbot
sudo ln -s /snap/bin/certbot /usr/bin/certbot
