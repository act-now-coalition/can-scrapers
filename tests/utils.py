import glob
from subprocess import STDOUT, check_output
import pathlib
import re

from typing import Optional

from can_tools import ACTIVE_SCRAPERS


class ModifiedClassSelector:
    """Selects tests to run based on modified files"""

    def __init__(
        self,
        base_branch: Optional[str] = "main",
        modules_path: Optional[str] = "can_tools/scrapers/**/*.py",
    ):
        self.base_branch = base_branch
        self.modules_path = modules_path

    @property
    def all_modified_files(self, base_branch: Optional[str] = None):
        """Set of all modified files"""
        base_branch = base_branch or self.base_branch
        cmd = "git", "diff", "--name-only", base_branch
        changed_files: bytes = check_output(cmd, stderr=STDOUT)
        changed_files = changed_files.decode("utf-8").replace("\\", "/")
        changed_files = changed_files.splitlines(False)
        changed_files = [cf for cf in changed_files if cf.strip()]
        return set(changed_files)

    @property
    def selected_files(self):
        """Set of modified files within the scope of the modules path"""
        all_files = set(glob.glob(self.modules_path, recursive=True))
        files = {file for file in all_files if "__init__.py" not in file}
        return files.intersection(self.all_modified_files)

    def get_classes_to_test(self):
        """Returns a list of classes to test based on which files have been modified"""
        classes = []
        for file in self.selected_files:
            module = pathlib.Path(__file__).parents[1] / file
            cmd = "grep", "class", module
            raw_class_names = check_output(cmd).splitlines()
            raw_class_names = [rc.decode("utf-8").split()[1] for rc in raw_class_names]
            class_names = {re.findall(r"^[^\(]*", cls)[0] for cls in raw_class_names}

            active_scraper_names = {cls.__name__ for cls in ACTIVE_SCRAPERS}
            class_names_to_test = class_names.intersection(active_scraper_names)
            classes.extend(class_names_to_test)

        return [cls for cls in ACTIVE_SCRAPERS if cls.__name__ in classes]
