import os
import pathlib
import re

import jinja2
import us
from InquirerPy import prompt
import us


def camel_to_snake(name):
    name = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", name).lower()


DIR = pathlib.Path(os.path.dirname(__file__))
SCRAPER_DIR = DIR.parent / "can_tools" / "scrapers" / "official"

ENV = jinja2.Environment(
    loader=jinja2.FileSystemLoader(DIR / "templates"),
)

scraper_question = [
    {
        "type": "list",
        "choices": ["Tableau", "Power BI", "ArcGIS"],
        "message": "What type of scraper are you buliding?",
        "name": "scraper_type",
    }
]

tableau_questions = [
    {
        "type": "input",
        "name": "name",
        "message": "What is the name of the scraper?",
    },
    {
        "type": "input",
        "name": "source",
        "message": "What is URL containing dashboard?",
    },
    {
        "type": "input",
        "name": "source_name",
        "message": "What is name of website/department/entity that maintains/publishes the website?",
    },
    {
        "type": "input",
        "name": "state_name",
        "message": "Which state is this scraper for?",
    },
    {
        "type": "input",
        "name": "baseurl",
        "message": "What is the baseurl of the Tableau dashboard?",
    },
    {
        "type": "input",
        "name": "viewPath",
        "message": "What is the view path for the specific dashboard?",
    },
    {
        "type": "input",
        "name": "data_tableau_table",
        "message": "What is the internal Tableau data table name?",
    },
    {
        "type": "input",
        "name": "location_name_col",
        "message": "Column name for county names in data set?",
    },
    {
        "type": "input",
        "name": "timezone",
        "message": "What timezone does this state use?",
    },
]


def main():
    scraper = prompt(scraper_question)["scraper_type"]
    if scraper == "Tableau":
        answers = prompt(tableau_questions)
        state = us.states.lookup(answers["state_name"])
        scraper_name = camel_to_snake(answers["name"]) + ".py"
        path: pathlib.Path = SCRAPER_DIR / state.abbr / scraper_name

        path.parent.mkdir(exist_ok=True, parents=True)
        with open(path, "w") as f:
            template = ENV.get_template("tableau.py")
            content = template.render(scraper=answers)
            f.write(content)

        print(f"Created scraper at {path}")
    else:
        raise NotImplementedError(f"Not ready for scraper of type {scraper}")


if __name__ == "__main__":
    main()
