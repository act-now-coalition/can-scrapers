from abc import ABC
from typing import Dict

import pandas as pd
import requests

from can_tools.scrapers.official.base import FederalDashboard


class HHSDataset(FederalDashboard, ABC):
    source: str
    source_name = "Department of Health and Human Services"
    provider = "hhs"
