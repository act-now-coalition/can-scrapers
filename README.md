# Covid Act Now Scraping Tools

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

- The `fetch` method is responsible for making network requests. It should request
  the remote resource and do as little else as possible. When the resource is a csv
  or json file, it is ok use `pd.read_XXX` as the body of the fetch method. Other cases
  might include the output of fetch being a `requests.Response` object, or other.
- The `normalize` method should transform the output of `fetch` page into scraped data
  and return a DataFrame with columns `(vintage, dt, location, category, measurement, unit, age, race, ethnicity, sex, value)`. See existing methods for
  examples
- The `validate` method should verify the data to ensure that it passes any
  necessary sanity checks.
- The `put` method takes a SQL connection and a DataFrame and then puts the
  DataFrame into the SQL database. This is taken care of by parent classes and
  does not need to be updated manuallly

Most scrapers will not require one to write the `validate` or put methods because
the generic methods should be able to validate the data and dump it into the database

All scrapers must inherit from `DatasetBase`, but this typically happens by subclassing
a resource specific parent class like `TableauDashboard` or `ArcGIS`.

## Triggering the integration

When creating a pull request various tests are performed on the code.
Occasionally, you might need to trigger a re-test without actually changing code
on your scraper. To achieve that goal, you can make an empty commit and then
push it, causing the `on: pull requests` checks to be run.

```git commit --allow-empty -m "Trigger GitHub actions"
git push
```

## Writing PowerBI Queries

Writing scrapers for Microsoft PowerBI Dashboards requires a POST request with a large JSON body.
Most of the queries can be found via inspecting network requests from the dashboard website itself,
and hardcoded into the code. Sometimes, you may have to modify the existing query.

### Request Body Structure

```
body : {
  version: "1.0.0",
  cancelQueries: [],
  modelId: string,
  queries: Query[]
}

Query : {
  "Query": Command[]
  "QueryId": string
  "ApplicationContext": AppContext
}

Command : {
  "SemanticQueryDataShapeCommand": SemanticCommand
}

SemanticCommand : {
  "Query": SemanticQuery
}

SemanticQuery: {
  "Version": int
  "From": FromClause[]
  "Select": (Selection|Aggregation|Measure)[]
}

FromClause: {
  "Name": string,
  "Entity": string
  "Type": string
}

Selection: {
  "Column": Column,
  "Name": string
}

Aggregation: {
  "Expression": {
    "Column": Column,
    "Function": string
  },
  "Name": string
}

Measure: {
  "Measure": Column,
  "Name": string
}

Column: {
  "Expression": {
    "SourceRef": {
      "Source": string
    }
    "Property": string
  },
  "Name": string
}
```

The body of the JSON request has four parts, `version`, `cancelQueries`, `modelId` and `queries`.

`version` and `cancelQueries` never change and are always `"1.0.0"` and `[]` respectively.

`modelId` is retrieved on demand via the `MicrosoftBIDashboard.get_model_data(self, resource_key)` method.

`queries` is the real meat of the JSON. This is where "sql like" queries will be constructed and put.

### Constructing BI Queries

The `queries` field of the PowerBI POST JSON is an array of `Query` objects.

Typically, a scraper will only need to have 1 `Query` object, as all the wanted data can be retrieved with
a single query.

A `Query` has 3 fields: `Version`, `From`, and `Select`.

`Version` is almost always the raw int `2`.

The `From` attribute is an array of `FromClause` objects. This is where we tell which _tables_ from which we
want to select data from.

`Select`, as you might have guessed, is where we say which _variables_ we want to select from the _tables_
we specific in the `From` attribute.

#### Constructing From

In order to specify a table to select, we provide a number of `FromClause` objects. There is a helper funtion in the `MicrosoftBIDashboard` class to do this (`construct_form`). The `construct_form` method has 1 argument, `nets`, which is an array of tuples `(n,e,t)`. These values correspond to "Name", "Entity" and "Type".

Let's take a look at an example request from the [Pennsylvania Department of Health dashboard](https://app.powerbigov.us/view?r=eyJrIjoiZGEwZjk0MjMtZjRiZS00Njc2LWIyNDItNGVjZjRmNDlkMDAyIiwidCI6IjQxOGUyODQxLTAxMjgtNGRkNS05YjZjLTQ3ZmM1YTlhMWJkZSJ9)

```json
...
"SemanticQueryDataShapeCommand": {
  "Query": {
    "Version": 2,
    "From": [
      {
        "Name": "c",
        "Entity": "Counts of Vaccinations by County of Residence",
        "Type": 0
      },
    ],
...
```

Here we can see that 1 table is being requested. The "Counts of Vaccinations by County of Residence".

To Replicate this `FromClause` we need to pass the correct tuple into the `construct_from`
method. For the table, the tuple would be `("c", "Counts of Vaccinations by County of Residence", 0)`, taking
the `Name`, `Entity`, and `Type` attributes.

Passing this tuple to the `construct_from` method will result in the same JSON as the example above:

```python
json = MicrosoftBIDashboard.construct_from([
  ("c", "Counts of Vaccinations by County of Residence", 0),
])

json == [
  {
    "Name": "c",
    "Entity": "Counts of Vaccinations by County of Residence",
    "Type": 0
  }
]
```

#### Constructing Select

The `MicrosoftBIDashboard` class has a helper function, similar to `construct_from`, for constructing this section:
`construct_select`. This method has 3 arguments: `sels`, `aggs`, and `meas`.

`sels` is a list of tuples containing 3 values: `(s, p, n)` which stand for Source, Property and Name

`aggs` is a list of tuples containing 4 values: `(s, p, f, n)` which stand for Source, Property, Function,
and Name

`meas` is a list of tuples containing 3 values: `(s, p, n)` which stand for Source, Property and Name

Let's continue with our example from before:

```json
...
"SemanticQueryDataShapeCommand": {
"Query": {
  "Version": 2,
  "From": ...,
  "Select": [
    {
      "Column": {
        "Expression": { "SourceRef": { "Source": "c" } },
        "Property": "date"
      },
      "Name": "covid-immunizations-county.date"
    },
    {
      "Aggregation": {
        "Expression": {
          "Column": {
            "Expression": { "SourceRef": { "Source": "c" } },
            "Property": "partial"
          }
        },
        "Function": 0
      },
      "Name": "Sum(covid-immunizations-county.partial)"
    },
    {
      "Aggregation": {
        "Expression": {
          "Column": {
            "Expression": { "SourceRef": { "Source": "c" } },
            "Property": "full"
          }
        },
        "Function": 0
      },
      "Name": "Sum(covid-immunizations-county.full)"
    }
  ],
...
```

---

The first object in the `"Select"` attribute is a _selection_ object. It simply requests the raw data
with no aggregations. Let's break it apart so we can see how it fits into our `construct_select` method.

We see that the first object in the Select array has a `Column` attribute. This is what sets it apart as a _sel_.
For a _sel_ we want 3 things: a Source, a Property, and a Name.

The source of this _sel_ we can get from `obj.Column.Expression.SourceRef.Source` and is `"c"`.

The Property we find from `obj.Column.Property` and is `"date"`

And the Name we find from `obj.Name` and is `"covid-immunizations-county.date"`

Using these, we can construct a _sel_ tuple that can be passed to the `construct_select` method:

```python
("c", "date", "covid-immunizations-county.date")
```

---

Moving on the the next object, we see it has an `Aggregation` attribute. This is how we know it's an _agg_.

Remember that an _agg_ has 4 attributes: Source, Property, Function, and Name.
We can map these attributes to a new _agg_ tuple:

```python
("c", "partial", 0, "Sum(covid-immunizations-county.partial)")
```

This _sel_ tuple is telling us we want to SELECT the property "partial" FROM the table "c" using the agg method defined by Function and Name (Sum)

We see that the next object is also an _agg_ so we'll construct that tuple in the
same manner:

```python
("c", "full", 0, "Sum(covid-immunizations-county.full)")
```

---

Now that we have our _sels_ and _aggs_, we can construct our query:

```python
"Query": {
    "Commands": [
        {
            "SemanticQueryDataShapeCommand": {
                "Query": {
                    "Version": 2,
                    "From": self.construct_from(
                      [
                        ("c", "Counts of Vaccinations by County of Residence", 0),
                      ]
                    ),
                    "Select": self.construct_select(
                        [
                            ("c", "date", "covid-immunizations-county.date")
                        ],
                        [
                          ("c", "partial", 0, "Sum(covid-immunizations-county.partial)"),
                          ("c", "full", 0, "Sum(covid-immunizations-county.full)")
                        ],
                        [],
                    ),
                }
            }
        }
    ]
},
```

This query is now saying: FROM the table "c" SELECT "date" and AGGREGATE "partial" and "full".

The measures array (the last argument in the `construct_select` method) is empty in this example.

Some requests may use this array. We haven't figured out exactly the
difference between a `Measure` and an `Aggregation`, but they differ only
slightly.

---

The next part of the BODY we need to construct is the `ApplicationContext`.
This object is telling the PowerBI backed from which dataset and report source
we are requesting.

The helper method `construct_application_context` is used here and has 2 arguments: `ds_id` and `report_id`. Don't worry about hunting
these down. The helper method `get_model_data` returns these values.

```python
"Query": {
    "Commands": ...,
    "ApplicationContext": self.construct_application_context(ds_id, report_id)
},
```

---

With everything ready, we can construct our full JSON body:

```python
self._setup_sess()
dashboard_frame = self.get_dashboard_iframe()
resource_key = self.get_resource_key(dashboard_frame)
ds_id, model_id, report_id = self.get_model_data(resource_key)

body = {}

# Set version
body["version"] = "1.0.0"
body["cancelQueries"] = []
body["modelId"] = model_id

body["queries"] = [
  {
    "Query": {
      "Commands": [
          {
            "SemanticQueryDataShapeCommand": {
                "Query": {
                    "Version": 2,
                    "From": self.construct_from(
                      [
                        ("c", "Counts of Vaccinations by County of Residence", 0),
                      ]
                    ),
                    "Select": self.construct_select(
                        [
                            ("c", "date", "covid-immunizations-county.date")
                        ],
                        [
                          ("c", "partial", 0, "Sum(covid-immunizations-county.partial)"),
                          ("c", "full", 0, "Sum(covid-immunizations-county.full)")
                        ],
                        [],
                    ),
                }
            }
          }
      ]
    },
    "QueryId": "",
    "ApplicationContext": self.construct_application_context(
        ds_id, report_id
    ),
  }
]
```
