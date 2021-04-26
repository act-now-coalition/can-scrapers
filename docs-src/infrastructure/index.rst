CAN Scraper Infrastructure
===========================

In this section you will learn more about the CAN Data Engineering
infrastructure.

All CAN scraper infrastructure is hosted in Google Cloud Platform and managed
with [terraform](https://www.terraform.io/)

In this repository there are two root-level directories that contain our cloud
resources:

1. ``infrastructure``: contains the terraform codes necessary to deploy and destroy
   the services
2. ``services``: A collection of microservices. Each microservice is contained in
   its own directory and deployed inside docker containers. We use
   [earthly](https://earthly.dev/) as a build tool for building all the


Infrastructure
--------------

The infrastructure for can-scrapers is a subset of that used for Covid County Data (CCD)

The CCD infrastructure is documented in the files below

We may work on dedicated notes/docs for the can-scrapers infrastructure, but we have not yet

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   overview

Services
--------------

Each microservice has config and supporting files in a subdirectory of ``services/``

Many of these services were used to support Covid County Data (CCD) before CCD merged with CAN

We will not document these here, but instead focus on the ones actively in use for CAN purposes

The services used to support CAN work are:

- ``prefect``: core orchestration system responsible for executing all scrapers
- ``webhook``: A webhook server that listens for push events to the main branch of this repo and updates the flows managed by prefect to always match master. This includes auto-updating of the codebase and auto-deployment of new scrapers
