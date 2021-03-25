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


def all_subclasses(cls):
    return set(cls.__subclasses__()).union(
        [s for c in cls.__subclasses__() for s in all_subclasses(c)]
    )


def non_abc_subclasses(cls):
    out = list(x for x in all_subclasses(cls) if not inspect.isabstract(x))

    # need to specifically remove some classes :(
    from can_tools.scrapers.official.base import TableauDashboard
    from can_tools.scrapers.official.PA.pa_vaccines import \
        PennsylvaniaVaccineDemographics

    bad = [TableauDashboard, PennsylvaniaVaccineDemographics]

    return [x for x in out if x not in bad]


ALL_SCRAPERS: List[Type[scrapers.DatasetBase]] = non_abc_subclasses(
    scrapers.DatasetBase
)
