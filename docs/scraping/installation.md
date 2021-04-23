# Developer Installation

This page provides information to help developers get their python environment
set up.

The first step will be to use git to clone the can-scrapers repository.

> If you have not installed git, please install it either from the [official
> instructions](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)
> or by installing [GitHub Desktop](https://desktop.github.com/)

```shell
git clone https://github.com/covid-projections/can-scrapers.git
```

We recommend that you use a Python virtual environment to keep your work on the
can_scrapers tooling separate from your system default Python environment or
environments used for other projects.


## Using Conda

To use Conda manage a virtual environment for this project, follow these steps:

> Note: all instructions of the form `$ something` require that you open your
> shell/command prompt/terminal and execute the command `something`

1. Install [Anaconda](https://www.anaconda.com/products/individual) or
   [Miniconda](https://docs.conda.io/projects/conda/en/latest/user-guide/install/)
2. Create a new conda environment for this project: `$ conda create -n
   can-scrapers python=3.8`
3. Activate the conda environment: `$ conda activate can-scrapers`
4. Change directories to the "can-scrapers" repository you cloned previously: `$
   cd /path/to/can-scrapers` (note you will need to change `/path/to`)
5. Install the requirements: `$ pip install -r requirements-dev.txt`
6. Set up `can_tools` library in developer mode: `$ pip install -e .`
7. Deactivate and activate the conda environment one more time (we've found in
   practice that the recently installed libraries/tools aren't immediately
   available after installation): `$ conda deactivate` and then `$ conda
   activate can-scrapers`


At this point you should be fully set up with a development environment suitable
for working on this project

You can test your setup by running some of the tests.

For example, to run the tests for the `Massachusetts` run `$ pytest -v -k Mass .`


### Optional Steps

Below are a few optional steps/tips that we've found help us be more efficient in our development process

#### Setup IPython profile

I (Spencer) often work with a text editor and IPython terminal. It is my happy place

To make things a bit more convenient when working on this project, I created a custom ipython-profile specifically for the can-scrapers conda environment

This custom ipython-profile does a few things:

- Imports common third-party packages like pandas and sqlalchemy
- Sets up a dev sqlite3 database and engine, bootstrapping it with all the metadata tables from `can_tools/bootstrap/*.csv` files
- Imports all scrapers
- Sets up autoreload so when I make an edit in my text editor, code is refreshed and I can simply re-execute corresponding method or function

Here's how I did it using conda on a unix based system (MacOS or Linux)

```sh
conda activate can-scrapers
cd $CONDA_PREFIX
mkdir -p ./etc/conda/activate.d
mkdir -p ./etc/conda/deactivate.d
touch ./etc/conda/activate.d/env_vars.sh && echo 'alias ipython="ipython --profile=can_scrapers"' >> ./etc/conda/activate.d/env_vars.sh
touch ./etc/conda/deactivate.d/env_vars.sh && echo 'unalias ipython' >> ./etc/conda/deactivate.d/env_vars.sh
```

I also created a file at `~/.ipython/profile_can_scrapers/ipython_config.py` with the following contents:

```python
c.InteractiveShellApp.exec_lines = [
    r"%autoreload 2",
    "import pandas as pd",
    "import sqlalchemy as sa",
    "import requests",
    "from can_tools.scrapers import *",
    "from can_tools.models import create_dev_engine, CovidObservation, Base, bootstrap, _bootstrap_csv_to_orm, CovidProvider, TemptableOfficialHasLocation, build_insert_from_temp",
    "engine, Session = create_dev_engine(verbose=False)",
]

c.TerminalIPythonApp.extensions = ["autoreload"]
```

Now when I activate my conda environment with `conda activate can-scrapers` and then start an ipython session with `ipython` all the code in `c.InteractiveShellApp.exec_lines` is automatically executed

If you want to customize or change what is run upon start of IPython, modify the lines of code in `c.InteractiveShellApp.exec_lines`
