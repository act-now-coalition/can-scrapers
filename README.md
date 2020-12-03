# Covid Act Now Scraping Tools


## Creating a development environment for scraping

1. Install `conda` (either [anaconda](https://www.anaconda.com/products/individual) or [miniconda](https://docs.conda.io/en/latest/miniconda.html))
2. Create conda environment for this project, `conda create -n can-scrapers python=3.6`
3. Activate the environment, `conda activate can-scrapers`
4. Move your command line or terminal into the `can-scrapers` directory
6. Install required packages, `pip install -r requirements-dev.txt`
5. (Windows users only) Install required package, `conda install fiona`
7. Install development version of package, `pip install -e .`


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
  measurement, unit, age, race, sex, value, provider)`.
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

