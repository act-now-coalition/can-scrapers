Writing Scrapers
================

.. contents:: Table of Contents
    :depth: 3


The goal of the ``can_tools`` package is to make it easy to *build* and *maintain* COVID data scrapers

As noted in :ref:`scraper_structure`, we have built a number of tools in an effort to achieve this goal

This document describes how to build a scraper

We will analyze the code for the ``NewJerseyVaccineCounty`` scraper, repeated below for convenience:

.. literalinclude:: ../../can_tools/scrapers/official/NJ/nj_vaccine.py
   :language: python
   :linenos:

Finding the right subclass
--------------------------

As we've scraped data for the past year, we've noticed that a few technologies are used to create the majority of COVID data dashboards

For the technologies we have come across often, we have created classes that capture key paterns of interaction that can be reused by specific scrapers

The first step when starting a new scraper is to determine the technology used to create the dashboard, and then find the corresponding subclass

See :ref:`dashboard_parent_classes` for a discussion of these classes

In our example with the `New Jersy Vaccine Scraper <https://covid19.nj.gov/#live-updates>`_, we observed that the dashboard was produced using ArcGIS

For that reason ``NewJerseyVaccineCounty`` subclasses from ``ArcGIS``

Filling in class level attributes
----------------------------------

After determining the type of dashboard you are planning to scrape and subclassing the appropriate parent, the next is to define some key class-level attributes

The ones that must be defined include:

- ``has_location: bool``: whether there is a column called ``location`` containing fips code (when ``has_location = True``) or a column called ``location_name`` containing county names (when ``has_location = False``)
- ``location_type: str``: The type of geography represented in the data. Typically this will be ``county`` because we aim to scrape state level dashboards that report county level data
- ``state_fips: int``: The (at most) two digit fips code for the  state as an integer. This should be found using the method ``us.states.lookup`` as shown in the example above
- ``source: str``: A URL pointing to the dashboard
- ``source_name: str``: The name of the entity that maintains or publishes the dashboard

We also reccomend that you define a mapping from column names to instances of ``CMU`` as a class-level attribute called variables (see :ref:`writing_normalize` below for more details)

There are other class-level attributes that may be required by the dashboard type specific parent class

For example, for the ``NewJerseyVaccineCounty`` class' parent -- ``ArcGIS`` -- the following are required:

- ``ARCGIS_ID: str``: A string identifying the resource id within the ArcGIS system
- ``service: str`` A string representing the name of the service containing the data

.. _writing_fetch:

Writing the fetch method
--------------------------

The first method you will write is ``fetch``

This is responsible for making a network request to fetch a remote resource

It should not handle parsing or cleaning of the response (other than a simple thing like parsing the JSON body of a ``requests.Response``)

The reason for this is that when scrapers are run, we like to keep track of failures for network requests vs failures in parsing or validation

For many dashboard types, this fetch method will be very simple and be a call to one or more of the helper methods defined in the dashboard type specific parent class

This is the case in our ``NewJerseyVaccineCounty`` scraper where we call out to the ``get_all_jsons`` method defined in ``ArcGIS``

.. literalinclude:: ../../can_tools/scrapers/official/NJ/nj_vaccine.py
   :language: python
   :pyobject: NewJerseyVaccineCounty.fetch

.. _writing_normalize:

Writing the normalize method
------------------------------

The normalize method is responsible for converting the raw data obtained by ``fetch`` into a clean, structured pandas DataFrame

This step is often the most work as it requires interpreting/understanding the data presented on the dashboard, understanding the desired DataFrame structure/schema, and writing the necessary data transformation code (usually a sequence of calls to pandas.DataFrame methods) to map from the raw form into the can-scrapers schema

The output DataFrames *must* contain the following columns:

+---------------+----------------------+---------------------------------------------------+
|    Column     |         Type         |                    Description                    |
+===============+======================+===================================================+
| vintage       | ``pd.TimeStamp``     | UTC timestamp for when scraper runs               |
+---------------+----------------------+---------------------------------------------------+
| dt            | ``pd.TimeStamp``     | Timestamp capturing date for observed data        |
+---------------+----------------------+---------------------------------------------------+
| location      | ``int``              | County FIPS code                                  |
+---------------+----------------------+---------------------------------------------------+
| location_name | ``str``              | County name (if not location column)              |
+---------------+----------------------+---------------------------------------------------+
| category      | ``str``              | Category for variable (see below)                 |
+---------------+----------------------+---------------------------------------------------+
| measurement   | ``str``              | Measurement for variable (see below)              |
+---------------+----------------------+---------------------------------------------------+
| unit          | ``str``              | Unit for variable (see below)                     |
+---------------+----------------------+---------------------------------------------------+
| age           | ``str``              | age group for demographic group (see below)       |
+---------------+----------------------+---------------------------------------------------+
| race          | ``str``              | race group for demographic group (see below)      |
+---------------+----------------------+---------------------------------------------------+
| ethnicity     | ``str``              | ethnicity group for demographic group (see below) |
+---------------+----------------------+---------------------------------------------------+
| sex           | ``str``              | sex group for demographic group (see below)       |
+---------------+----------------------+---------------------------------------------------+
| value         | ``int`` or ``float`` | The observed value                                |
+---------------+----------------------+---------------------------------------------------+

The ``vintage``, ``dt``,  and ``location`` (or ``location_name`` depending on whether the dashboard reports fips codes or county names) columns are typically added using the ``_rename_or_add_date_and_location`` (from ``StateDashboard``)

CovidVariables
***************

The (``category``, ``measurement``, ``unit``) triplet define what type of data is being observed.

- ``category`` describes `what` the variable is. Some examples are ``total_deaths`` or ``total_vaccine_completed``, etc.
- ``measurement`` describes `how` the data is being reported. Some examples are ``new``, ``cumulative``, ``rolling_7day_average``, etc.
- ``unit`` describes the units used for the observation. Some examples are ``people``, ``percentage``, ``specimens``, ``doses``, etc.

The values used for these three columns must be known in our system

Known values are recorded in the file ``can_tools/bootstrap/covid_variables.csv``

Most often, you will be trying to scrape variables that we already know about. In this case, you need to go to the csv file mentioned above and find a row that matches what you are looking for


CovidDemographics
********************

The (``age``, ``race``, ``ethnicity``, ``sex``) 4-tuple define what type of data is being observed.

- ``age`` Categorizes an age group. Examples are ``all``, ``0-16``, ``20-30``, ``81_plus``, etc.
- ``race`` describes the race of the subpopulation represented. Some examples are ``all``, ``ai_an``, ``asian``, ``black``, etc.
- ``ethnicity`` describes the ethnicity of the subpopulation represented. Some examples are ``all``, ``hispanic``, ``unknown`` and  ``non-hispanic``
- ``sex`` describes the sex of the subpopulation represented. Possible values are ``all``, ``male``, ``female``, ``unknown``

This 4-tuple describes the demographic group represented in the data

The reported demographic group must be known in our system

Known values are recorded in the file ``can_tools/bootstrap/covid_demographics.csv``


.. note::

    When scraping data that does not have a demographic dimension, these columns will all be filled entirely with the string ``all``


``CMU``
*********

To help fill in the values for the variable dimensions (category, measurement, unit) and the demographic dimensions (age, race, ethnicity, sex); there is a helper class called ``CMU``.

.. note::

   Before we added demographics to our system, we only had the variable dimensions. The name ``CMU`` was chosen as an acronym for (catgegory, measurement, unit)

The ``CMU`` class is documented below:

.. autoclass:: can_tools.scrapers.base.CMU


Typically a scraper will define a class attribute called ``variables`` that is a dictionary mapping from a column name in a wide form dataset we receive from the source into an instance of CMU describing variable and demographic dimensions for the data.

A few ``CMU`` come up in many scrapers. These include ``CMU`` for total people with at least one dose, total people fully vaccinated, etc. Instead of repeating the instantiation of these ``CMU`` instances in every scraper, we instead have a helper module ``can_tools.scrapers.variables`` that contains common definitions as module level constants

These were used in the ``NewJerseyVaccineCounty`` scraper we've been working with:


.. literalinclude:: ../../can_tools/scrapers/official/NJ/nj_vaccine.py
   :language: python
   :start-after: # start variables
   :end-before: # end variables


Helper Methods
****************

Now we have all the pieces we need in order to fill in the necessary rows of a normalized DataFrame

There are a few helper methods on the ``StateDashboard`` class (and therefore its subclasses) that we often use: ``_rename_or_add_date_and_location`` and ``_reshape_variables``

These are documented in  :ref:`state_dashboard` and shown in the ``NewJerseyVaccineCounty.normalize`` method below:

.. literalinclude:: ../../can_tools/scrapers/official/NJ/nj_vaccine.py
   :language: python
   :pyobject: NewJerseyVaccineCounty.normalize


Running the scraper locally
----------------------------

After writing a ``fetch`` and ``normalize`` method, you can run your scraper

We could do this as follows:

.. code:: python3

   from can_tools.scrapers import NewJerseyVaccineCounty

   # create scraper
   d = NewJerseyVaccineCounty()

# fetch raw resource
   raw = d.fetch()

   # normalize the raw resource into a conformable DataFrame
   df = d.normalize(raw)

At this point you should have a normalized DataFrame, ready to be injected into the CAN database.

While running locally, we suggest you create an in-memory sqlite database and attempt to ``put`` your data

We have helper methods set up for you to do this:

.. code:: python3

   from can_tools.models import create_dev_engine

   # create a sqlalchemy engine and session connected to
   # in memory sqlite db
   engine, Session = create_dev_engine()

   # put the DataFrame into the db
   d.put(engine, df)

If at this stage you have any problems inserting the data, see the :ref:`faq` page

Running the tests for the scraper
------------------------------------

There are a few tests that are automatically defined for you

We use the ``pytest`` framework

If you wanted to run the full test suite, you could run the ``pytest`` command from the ``can_tools`` directory

To select a subset of tests to run, use the ``-k`` flag for ``pytest``

For example, to run only the tests for our ``NewJerseyVaccineCounty`` scraper, I would run

.. code:: shell

   pytest -k NewJerseyVaccineCounty


This would produce output similar to

.. code::

   ❯ pytest -k NewJerseyVaccineCounty
   ============================================ test session starts =============================================
   platform linux -- Python 3.9.1, pytest-6.1.2, py-1.10.0, pluggy-0.13.1
   rootdir: /home/sglyon/valorum/covid/can-scrapers
   plugins: xdist-2.2.1, forked-1.3.0, parallel-0.1.0
   collected 370 items / 365 deselected / 5 selected

   tests/test_datasets.py .....                                                                           [100%]

   ============================================== warnings summary ==============================================
   ../../../anaconda3/envs/can-scrapers/lib/python3.9/site-packages/us/states.py:86: 46 warnings
   /home/sglyon/anaconda3/envs/can-scrapers/lib/python3.9/site-packages/us/states.py:86: DeprecationWarning: PY_SSIZE_T_CLEAN will be required for '#' formats
      val = jellyfish.metaphone(val)

   -- Docs: https://docs.pytest.org/en/stable/warnings.html
   =============================== 5 passed, 365 deselected, 46 warnings in 2.16s ===============================
