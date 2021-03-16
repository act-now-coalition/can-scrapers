from abc import ABC
import requests

from typing import Dict

import pandas as pd

from can_tools.scrapers.official.base import FederalDashboard


class HHSDataset(FederalDashboard, ABC):
    source: str
    source_name = "Department of Health and Human Services"
    provider = "hhs"
