#!/usr/bin/env bash

git pull origin main
/home/sglyon/miniconda/envs/prefect-can-scrapers/bin/pip install -r /home/sglyon/can-scrapers/requirements.txt
/home/sglyon/miniconda/envs/prefect-can-scrapers/bin/pip install -e /home/sglyon/can-scrapers
/home/sglyon/miniconda/envs/prefect-can-scrapers/bin/python /home/sglyon/can-scrapers/services/prefect/flows/generated_flows.py
