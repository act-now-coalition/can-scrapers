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
