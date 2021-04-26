Welcome to CAN Scrapers's documentation!
========================================

This documentation is intended for developers who wish to contribute to the
CovidActNow (CAN) data engineering effort. The main goal of the CAN data
engineering team is identify, collect, and distribute high-integrity COVID-19
data for all US counties. The main metrics of importance are cases, deaths,
testing statistics, hospitalizations, and vaccinations.

The efforts of the data engineering team are roughly split into two categories:

1. Data scraping and maintenance: writing code (typically Python) for collecting
   data from some official source like a website, dashboard, or downloadable
   file. This is mainly done by a large group of individual contributors, each
   being primarily responsible for a subset of scrapers.
2. Infrastructure and DevOps: managing the running of scrapers, maintaining the
   CAN database, maintaining APIs, and working on CI/CD pipelines. This job is
   mainly done by the CAN data engineering leads.

The organization of the documentation follows the categories outlined above.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   scraping/index
   infrastructure/index

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
