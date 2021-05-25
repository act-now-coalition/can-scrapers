# Covid Act Now Scraping Tools

developer-docs: https://covid-projections.github.io/can-scrapers/index.html


## Creating a development environment for scraping

1. Install `conda` (either [anaconda](https://www.anaconda.com/products/individual) or [miniconda](https://docs.conda.io/en/latest/miniconda.html))
2. Create conda environment for this project, `conda create -n can-scrapers python=3`
3. Activate the environment, `conda activate can-scrapers`
4. Move your command line or terminal into the `can-scrapers` directory
5. (Windows/Mac users only) Install required package, `conda install fiona`
6. Install required dependencies, `pip install -e .`

### Database setup

Our production database is an instance of PostgreSQL on google cloud SQL.

All of our SQL setup and interactions happen through sqlalchemy, which is
(mostly) database engine agnostic.

For running integration tests locally there are two options:

1. Use an in-memory sqlite backend: This is the default setup and will happen
   automatically for you when you run `pytest` operations from the local
   directory (see training/training.org for more info on pytest options).
2. Use a PostgreSQL server: To use a PostgreSQL server instead, you must set the
   environment variable `CAN_PG_CONN_STR` to a proper PostgreSQL connection URI
   before running pytest. Again see training/training.org for more info


If you would like to work interactively in an IPython session or Jupyter
notebook, you can use the function `can_tools.models.create_dev_engine` to set
up an in-memory SQLite instance

Below is a code snippet that sets this up, and then runs the `Florida` scraper
and inserts data into the database

```python
from can_tools.models import create_dev_engine
from can_tools.scrapers import Florida

# setup databsae
engine, Session = create_dev_engine()

scraper = Florida()
df = scraper.normalize(scraper.fetch())

scraper.put(engine, df)
```

Note that by default the `create_dev_engine` routine will construct the database
in a verbose mode where all SQL commands are echoed to the console. We find that
this is helpful while debugging and developing. This can be disabled by passing
`verbose=False` when calling `create_dev_engine`.

## Setting up VS Code

Steps to set up VS code:

1. Install `python` and `pylance` Visual Studio Code extensions
2. Reload Visual Studio Code
3. Open can-scrapers directory in Visual Studio Code
4. Select the `can-tools` conda environment as the workspace interpreter.

Please do not push any changes made to the `.vscode` directory. That has some
shared settings, but will also be overwritten by the absolute path to the
conda environment on your machine. This path is unlikely to match exactly
with the path for any other team members

## Organization of scrapers

The scrapers in this repository are organized in the `can_tools` python package

All scrapers are written in `can_tools/scrapers` directory

If the resource to be scraped comes from an official source (like a government web page or
health department) then the scraper goes into `can_tools/scrapers/official`. Inside the `official`
sub-directory there are many folders, each with the two letter abbreviation for a state. For
example, scrapers that extract data from North Carolina Deparment of Health are in 
`can_tools/scrapers/official/NC`


## Writing a new scraper

Behind the scenes of every scraper written in `can-tools` are abstract base
classes (ABC). These ABCs define abstract methods `fetch`, `normalize`, `validate`,
and `put` which must be implemented in order to create a scraper.

* The `fetch` method is responsible for making network requests. It should request
  the remote resource and do as little else as possible. When the resource is a csv
  or json file, it is ok use `pd.read_XXX` as the body of the fetch method. Other cases
  might include the output of fetch being a `requests.Response` object, or other.
* The `normalize` method should transform the output of `fetch` page into scraped data
  and return a DataFrame with columns `(vintage, dt, location, category,
  measurement, unit, age, race, ethnicity, sex, value)`. See existing methods for
  examples
* The `validate` method should verify the data to ensure that it passes any
  necessary sanity checks.
* The `put` method takes a SQL connection and a DataFrame and then puts the
  DataFrame into the SQL database. This is taken care of by parent classes and
  does not need to be updated manuallly

Most scrapers will not require one to write the `validate` or put methods because
the generic methods should be able to validate the data and dump it into the database

All scrapers must inherit from `DatasetBase`, but this typically happens by subclassing 
a resource specific parent class like `TableauDashboard` or `ArcGIS`.

## Triggering the integration

When creating a pull request various tests are performed on the code.
Occasionally, you might need to trigger a re-test without actually changing code
on your scraper.  To achieve that goal, you can make an empty commit and then
push it, causing the `on: pull requests` checks to be run.

```git commit --allow-empty -m "Trigger GitHub actions"
git push
```
