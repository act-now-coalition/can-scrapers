from can_tools.scrapers.official.AZ import (
    ArizonaData,
)

from can_tools.scrapers.official.CA import (
    CaliforniaCasesDeaths,
    CaliforniaHospitals,
    CaliforniaTesting,
)

from can_tools.scrapers.official.DC import DCCases, DCGeneral, DCDeaths

from can_tools.scrapers.official.DE import (
    DelawareData,
    DelawareStateVaccine,
)

from can_tools.scrapers.official.federal import (
    CDCCovidDataTracker,
    CDCStateVaccine,
    HHSReportedPatientImpactHospitalCapacityFacility,
    HHSReportedPatientImpactHospitalCapacityState,
)
from can_tools.scrapers.official.FL import (
    Florida,
    FloridaHospitalCovid,
    FloridaHospitalUsage,
    FloridaICUUsage,
    FloridaCountyVaccine,
)

from can_tools.scrapers.official.PA import (
    PennsylvaniaCasesDeaths,
    PennsylvaniaCountyVaccines,
    PennsylvaniaHospitals,
)

# from can_tools.scrapers.official.IL import IllinoisDemographics, IllinoisHistorical
from can_tools.scrapers.official.IL.il_vaccine import (
    IllinoisVaccineState,
    IllinoisVaccineCounty,
)
from can_tools.scrapers.official.MI import MichiganVaccineCounty
from can_tools.scrapers.official.MD import (
    MarylandCounties,
    MarylandState,
)

# from can_tools.scrapers.official.MA import Massachusetts
from can_tools.scrapers.official.NC import NorthCarolinaVaccineCounty
from can_tools.scrapers.official.NJ import NewJerseyVaccineCounty
from can_tools.scrapers.official.NY import NewYorkTests
from can_tools.scrapers.official.TN import (
    TennesseeAge,
    TennesseeAgeByCounty,
    TennesseeCounty,
    TennesseeRaceEthnicitySex,
    TennesseeState,
)
from can_tools.scrapers.official.TX import (
    TexasCasesDeaths,
    TexasTests,
    TexasCountyVaccine,
    TexasStateVaccine,
    # TexasVaccineDemographics,
)
from can_tools.scrapers.official.WI import WisconsinCounties, WisconsinState

from can_tools.scrapers.official.CT import (
    CTCountyDeathHospitalizations,
    CTCountyTests,
    CTState,
)

from can_tools.scrapers.official.OH import OhioVaccineCounty

from can_tools.scrapers.official.VT import VermontCountyVaccine, VermontStateVaccine
