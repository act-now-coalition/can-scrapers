# The MIT License (MIT)
#
# Copyright (c) 2020 Covid Act Now
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
"""Covid Act Now
"""

__version__ = "0.0.1"
import inspect
from typing import List, Type

from can_tools import scrapers
from can_tools.scrapers.official.base import TableauDashboard
from can_tools.scrapers.official.PA.pa_vaccines import PennsylvaniaVaccineDemographics
from can_tools.scrapers.official.federal.CDC.cdc_testing import CDCTestingBase


def all_subclasses(cls):
    return set(cls.__subclasses__()).union(
        [s for c in cls.__subclasses__() for s in all_subclasses(c)]
    )


def unblocked_scrapers(cls):
    # TableauDashboard, CDCTestingBase and PennsylvaniaVaccineDemographics
    # are base classes that are not functional scrapers, so we explicitly remove them.
    blocked = [PennsylvaniaVaccineDemographics, TableauDashboard, CDCTestingBase]
    return list(
        x for x in all_subclasses(cls) if not inspect.isabstract(x) and x not in blocked
    )


ALL_SCRAPERS: List[Type[scrapers.DatasetBase]] = unblocked_scrapers(
    scrapers.DatasetBase
)


# Data sources currently used by the CAN site/API to be included in Prefect flows.
# We're currently minimizing our list of active scrapers in order to reduce our Prefect costs.
# Last updated 08/01/2022
ACTIVE_SCRAPERS = [
    # National/federal dataset scrapers
    scrapers.USAFactsCases,
    scrapers.USAFactsDeaths,
    scrapers.HHSReportedPatientImpactHospitalCapacityFacility,
    scrapers.HHSReportedPatientImpactHospitalCapacityState,
    scrapers.HHSTestingState,
    scrapers.CDCCommunityLevelMetrics,
    scrapers.CDCHistoricalCountyVaccine,
    scrapers.CDCOriginallyPostedTestingDataset,
    scrapers.CDCHistoricalTestingDataset,
    scrapers.CDCUSAVaccine,
    scrapers.CDCStateVaccine,
    scrapers.CDCStateCasesDeaths,
    scrapers.CDCCountyCasesDeaths,
    # State- and county-specific scrapers
    scrapers.PhiladelphiaVaccine,
    scrapers.PennsylvaniaCountyVaccines,
    scrapers.CaliforniaVaccineCounty,
    scrapers.CaliforniaTestPositivity,
    scrapers.NewMexicoVaccineCounty,
    scrapers.VirginiaVaccine,
    scrapers.VermontCountyVaccine,
    scrapers.NCVaccineCounty,
    scrapers.NCVaccineState,
]
