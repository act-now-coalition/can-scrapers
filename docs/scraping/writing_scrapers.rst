Writing Scrapers
================

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

To help fill in the values for the

Running the scraper locally
----------------------------


Running the tests for the scraper
------------------------------------
