CAN Scraper Infrastructure
===========================

In this section you will learn more about the CAN Data Engineering
infrastructure.

All CAN scraper infrastructure is hosted in Google Cloud Platform and managed
with [terraform](https://www.terraform.io/)

In this repository there are two root-level directories that contain our cloud
resources:

1. `infrastructure`: contains the terraform codes necessary to deploy and destroy
   the services
2. `services`: A collection of microservices. Each microservice is contained in
   its own directory and deployed inside docker containers. We use
   [earthly](https://earthly.dev/) as a build tool for building all the

Please use the table of contents below to learn more about each part of
infrastructure

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   overview
