import glob
from subprocess import STDOUT, check_output
import pathlib
import re

from typing import List, Set

from can_tools import ACTIVE_SCRAPERS
from can_tools.scrapers.base import DatasetBase


def get_files_diff(base_branch: str, modules_path: str) -> Set[str]:
    """Set of modified files within the scope of the modules path"""
    cmd = "git", "diff", "--name-only", base_branch
    changed_files: bytes = check_output(cmd, stderr=STDOUT)
    changed_files = changed_files.decode("utf-8").replace("\\", "/")
    changed_files = changed_files.splitlines(False)
    changed_files = {cf for cf in changed_files if cf.strip()}

    files_in_scope: Set[str] = set(glob.glob(modules_path, recursive=True))
    return files_in_scope.intersection(changed_files)


def get_modified_scrapers(
    base_branch: str = "main", modules_path: str = "can_tools/scrapers/**/*.py"
) -> List[DatasetBase]:
    """Returns a list of scraper classes to test based on which files have been modified"""
    modified_files = get_files_diff(base_branch=base_branch, modules_path=modules_path)
    modified_files = {mf for mf in modified_files if "__init__.py" not in mf}
    classes = []
    for file in modified_files:
        module = pathlib.Path(__file__).parents[1] / file
        cmd = "grep", "class", module
        raw_class_names = check_output(cmd).splitlines()
        raw_class_names = [rc.decode("utf-8").split()[1] for rc in raw_class_names]
        class_names = {re.findall(r"^[^\(]*", cls)[0] for cls in raw_class_names}

        active_scraper_names = {cls.__name__ for cls in ACTIVE_SCRAPERS}
        class_names_to_test = class_names.intersection(active_scraper_names)
        classes.extend(class_names_to_test)

    return [cls for cls in ACTIVE_SCRAPERS if cls.__name__ in classes]
