#!/usr/bin/env bash

git pull origin main
/home/sglyon/miniconda/envs/prefect-can-scrapers/bin/python /home/sglyon/can-scrapers/services/prefect/flows/generated_flows.py
