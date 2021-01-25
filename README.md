# Covid Act Now Scraping Tools


## Creating a development environment for scraping

1. Install `conda` (either [anaconda](https://www.anaconda.com/products/individual) or [miniconda](https://docs.conda.io/en/latest/miniconda.html))
2. Create conda environment for this project, `conda create -n can-scrapers python=3.6`
3. Activate the environment, `conda activate can-scrapers`
4. Move your command line or terminal into the `can-scrapers` directory
6. Install required packages, `pip install -r requirements-dev.txt`
5. (Windows users only) Install required package, `conda install fiona`
7. Install development version of package, `pip install -e .`

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

## Writing a new scraper

Behind the scenes of every scraper written in `can-tools` are abstract base
classes (ABC). These ABCs define abstract methods `fetch`, `normalize`, `validate`,
and `put` which must be implemented in order to create a scraper.

* The `fetch` method should grab the data and return the raw HTML page.
* The `normalize` method should transform the raw HTML page into scraped data
  and return a DataFrame with columns `(vintage, dt, location, category,
  measurement, unit, age, race, ethnicity, sex, value, provider)`.
* The `validate` method should verify the data to ensure that it passes any
  necessary sanity checks.
* The `put` method takes a SQL connection and a DataFrame and then puts the
  DataFrame into the SQL database.

Most scrapers will not require one to write the `validate` or put methods because
the generic methods should be able to validate the data and dump it into the database

The two core ABCs are `DatasetBaseDate` and `DatasetBaseNoDate`. The key
difference between these two ABCs is that the `get` method for `DatasetBaseDate`
expects to receive an argument `date` to it's `get` method whereas
`DatasetBaseNoDate` does not expect any arguments to the `get` method.
