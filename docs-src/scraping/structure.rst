.. _scraper_structure:

Structure
===============

Each scraper in ``can-scrapers`` is a Python class within the ``can_tools/scrapers`` Python package

Organization
-------------

The scrapers are organized in a few sub-directories of ``can_tools/scrapers``:

- ``official/``: these contain data from official federal, state, or county government websites (including health departments, CDC, HHS, etc.).
    - Scrapers targeting a state level dashboard are put in ``official/XX`` where ``XX`` is the two letter state abbreviation (for example ``official/NM/nm_vaccine.py`` for a scraper collecting vaccine data for counties in the state of New Mexico)
    - Scrapers for a specific county are organized into ``official/XX/counties`` directory. For example ``official/XX/counties/la_county_vaccine.py`` might have a scraper that scrapes vaccine data from the Los Angeles county dashboard
- ``usafacts/``: scrapers for the county-level data provided by usafacts
- ``uscensus/``: scrapers that obtain demographic data from the US Census

Class Hierarchy
---------------

Let's consider an example scraper and its lineage: the ``NewJerseyVaccineCounty`` class found in ``can_tools/scrapers/official/NJ/nj_vaccine.py``

Let ``A <: B`` represent the phrase "A is a subclass of B"

Then the following is true about ``NewJerseyVaccineCounty``

.. code::

    NewJerseyVaccineCounty <: ArcGIS <: StateDashboard <: DatasetBase

Each of the parent classes has a specific purpose and adds in functionality

We'll start at the top of the hierarchy and work our way down

DatasetBase
*************

Each scraper must be a subclass of the core ``DatasetBase`` class.

The ``DatasetBase`` class is defined in ``can_tools/scrapers/base.py`` and does a number of things:

- Automatically generates a prefect flow for execution of the scraper in the production pipeline
- Abstracts away all non-scraper specific IO. This includes writing out temporary results, storing in cloud buckets, inserting into database, etc.
- Doing some common data quality checks (called `validation`)
- Defines helper methods for wranging data, these include methods ``extract_CMU``
- Defines a common interface that must be satisfied by all scrapers. These are abstract methods must be implemented by a subclass and include:
    - ``fetch``: responsible for doing network operations to collect data
    - ``normalize``: consumes the output of ``fetch`` and returns normalized data (see below)
    - ``put``: consumes output of ``normalize`` and stores into database


Most of our scrapers are from official government or health department websites. There are common tasks and configuration for all scrapers of this type

For this reason, there are other abstract classes that inherit from ``DatasetBase``

These include: ``StateDashbord``, ``CountyDashboard``, ``FederalDashboard``

We'll talk about these next

.. _state_dashboard:

StateDashboard
***************

The majority of our scrapers collect data from a state maintained dashboard

The ``StateDashboard`` class (defined in ``can_tools/scrapers/official/base.py``) adds some tools to make getting data from these sources easier:

- Defines ``table``, ``provider``, and ``data_type`` class attributes
- Methods ``put`` and ``_put_exec``: the code needed to push data to the database. Note, this means that none of our scraper classes (at the bottom of the hierarchy like ``NewJerseyVaccineCounty``) need to worry about database interactions
- Methods ``_rename_or_add_date_and_location`` and ``_reshape_variables``: tools for cleaning data (see below)

.. autofunction:: can_tools.scrapers.official.base.StateDashboard._rename_or_add_date_and_location

.. autofunction:: can_tools.scrapers.official.base.StateDashboard._reshape_variables


.. note::

    ``CountyDashboard`` and ``FederalDashboard`` inherit from ``StateDashboard`` and update the ``provider`` attribute. These are also defined in ``can_tools/scrapers/official/base.py``


.. _dashboard_parent_classes:

Dashboard Type subclasses
**************************

The next level in the hierarchy is a subclass for a specific type of dashboard technology

In the ``NewJerseyVaccineCounty`` example, this was the ``ArcGIS`` class

This subclass inherits from ``StateDashboard`` (so a scraper for an ArcGIS dashbaord only need to subclass ``ArcGIS`` and will get all goodies from ``StateDashboard`` and ``DatasetBase``) and adds in tools specific for interacting with ArcGIS dashboards

``ArcGIS`` has some siblings:

- ``SODA``: interacting with resources that adhere to the SODA standard
- ``TableauDashboard``: tools for extracting data from Tableau based dashboards
- ``MicrosoftBIDashboard``: tools for extracting data from Microsoft BI dashboards
- ``GoogleDataStudioDashboard``: tools for extracting data from Google Data Studio dashboards

In general, when you begin a new scraper, the initial steps are

1. Determine the technology used to create the dashboard
2. See if we have a subclass specific to that dashboard type
3. See examples of existing scrapers that build on that subclass to get a jump start on how to structure your new scraper


.. note::

    The technology-specific parent classes are defined in ``can_tools/scrapers/official/base.py``

.. _scraper_lifecycle:

Scraper Lifecycle
------------------

With all that in mind, we now lay out the lifecycle of a scraper when it runs in production

We will do this by writing code needed for running the scraper

.. code-block:: python
    :linenos:

    scraper = NewJerseyVaccineCounty()
    raw = scraper.fetch()
    clean = scraper.normalize(raw)
    scraper.validate(clean)
    scraper.put(engine, clean)

The line by line description of this code is

1. Create an instance of the scraper class. We can optionally pass ``execution_dt`` as an argument
2. Call the ``.fetch`` method to do network requests and get ``raw`` data. This method is typically defined directly in the child class
3. Call the ``.normalize(raw)`` method to get a cleaned DataFrame. This method is also typically defined directly in the child class. Implementing the ``.fetch`` and ``.normalize`` methods is the core of what we mean when we say "write a scraper"
4. Call the ``.validate(clean)`` method to check the data. There is a default ``validate`` method in ``DatasetBase``, but you can override if you know something specific needs to be checked for the scraper you are working on
5. Call ``.put(engine, clean)`` to store the data in the database backing the sqlalchemy Engine ``engine``. This is written in ``StateDashboard`` and should not need to be overwridden in child classes
