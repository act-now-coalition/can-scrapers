# Covid Act Now Scraping Tools


## Creating a development environment for scraping

1. Install `conda` (either anaconda or miniconda)
2. Create conda environment for this project `conda create -n can-tools python=3.8`
3. Activate the environment `conda activate can-tools`
4. Move your command line or terminal into the `can-tools` directory
5. If you are on Windows `conda install gdal` and `conda install fiona`
6. Install required packages `pip install -r requirements.txt`
7. Install development version of package `pip install -e .`


## Writing a new scraper

Behind the scenes of every scraper written in `can-tools` are two abstract base
classes (ABC). These two ABCs define abstract methods `get` and `put` which must
be implemented in order to create a scraper.

* The `get` method should fetch the scraped data and return a DataFrame with
  columns `(vintage, dt, location, category, measurement, unit, age, race, sex,
  value, provider)`.
* The `put` method takes a SQL connection and a DataFrame and then puts the
  DataFrame into the SQL database.

The two core ABCs are `DatasetBaseDate` and `DatasetBaseNoDate`. The key
difference between these two ABCs is that the `get` method for `DatasetBaseDate`
expects to receive an argument `date` to it's `get` method whereas
`DatasetBaseNoDate` does not expect any arguments to the `get` method.

