#!/usr/bin/env bash

git pull origin main
sudo apt install python3-pip
/home/sglyon/miniconda/envs/prefect-can-scrapers/bin/pip3 install -e /home/sglyon/can-scrapers
/home/sglyon/miniconda/envs/prefect-can-scrapers/bin/python /home/sglyon/can-scrapers/services/prefect/flows/generated_flows.py
