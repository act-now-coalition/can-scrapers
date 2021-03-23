from can_tools.scrapers.official.AZ import (
    ArizonaData,
    ArizonaMaricopaVaccine,
    ArizonaVaccineCounty,
    ArizonaVaccineCountyAllocated,
)
from can_tools.scrapers.official.CA import (
    CaliforniaHospitals,
    CaliforniaTesting,
    CaliforniaVaccineCounty,
    CASanDiegoVaccine,
    LACaliforniaCountyVaccine,
)
from can_tools.scrapers.official.CT import (
    CTCountyDeathHospitalizations,
    CTCountyTests,
    CTCountyVaccine,
    CTState,
)
from can_tools.scrapers.official.DC import DCCases, DCDeaths, DCGeneral, DCVaccineSex
from can_tools.scrapers.official.DE import DelawareData, DelawareStateVaccine
from can_tools.scrapers.official.federal import (
    CDCCovidDataTracker,
    CDCStateVaccine,
    CDCVariantTracker,
    HHSReportedPatientImpactHospitalCapacityFacility,
    HHSReportedPatientImpactHospitalCapacityState,
)
from can_tools.scrapers.official.FL import (
    Florida,
    FloridaCountyVaccine,
    FloridaHospitalCovid,
    FloridaHospitalUsage,
    FloridaICUUsage,
)
from can_tools.scrapers.official.GA import GeorgiaCountyVaccine

# from can_tools.scrapers.official.IL import IllinoisDemographics, IllinoisHistorical
from can_tools.scrapers.official.IL.il_vaccine import (
    IllinoisVaccineCounty,
    IllinoisVaccineState,
)
from can_tools.scrapers.official.IN import IndianaCountyVaccinations
from can_tools.scrapers.official.LA.la_county import LAVaccineCounty
from can_tools.scrapers.official.MD import (
    MarylandCounties,
    MarylandCountyVaccines,
    MarylandState,
)
from can_tools.scrapers.official.ME import MaineCountyVaccines
from can_tools.scrapers.official.MI import MichiganVaccineCounty
from can_tools.scrapers.official.MN import MinnesotaCountyVaccines
from can_tools.scrapers.official.MO import MissouriVaccineCounty
from can_tools.scrapers.official.MT import MontanaCountyVaccine, MontanaStateVaccine

# from can_tools.scrapers.official.MA import Massachusetts
from can_tools.scrapers.official.NC import NCVaccine
from can_tools.scrapers.official.NJ import NewJerseyVaccineCounty
from can_tools.scrapers.official.NM import NewMexicoVaccineCounty
from can_tools.scrapers.official.NV import NevadaCountyVaccines
from can_tools.scrapers.official.NY import NewYorkTests, NewYorkVaccineCounty
from can_tools.scrapers.official.OH import OhioVaccineCounty
from can_tools.scrapers.official.OR import OregonVaccineCounty
from can_tools.scrapers.official.PA import (
    PennsylvaniaCasesDeaths,
    PennsylvaniaCountyVaccines,
    PennsylvaniaHospitals,
    PennsylvaniaVaccineAge,
    PennsylvaniaVaccineEthnicity,
    PennsylvaniaVaccineRace,
    PennsylvaniaVaccineSex,
)
from can_tools.scrapers.official.TN import (
    TennesseeAge,
    TennesseeAgeByCounty,
    TennesseeCounty,
    TennesseeRaceEthnicitySex,
    TennesseeState,
    TennesseeVaccineCounty,
)
from can_tools.scrapers.official.TX import (
    TexasCasesDeaths,
    TexasCountyVaccine,
    TexasStateVaccine,
    TexasTests,
    TXVaccineCountyAge,
    TXVaccineCountyRace,
)
from can_tools.scrapers.official.VA import VirginiaVaccine
from can_tools.scrapers.official.VT import VermontCountyVaccine, VermontStateVaccine
from can_tools.scrapers.official.WA import WashingtonVaccine
from can_tools.scrapers.official.WI import (
    WisconsinCounties,
    WisconsinState,
    WisconsinVaccineAge,
    WisconsinVaccineCounty,
    WisconsinVaccineEthnicity,
    WisconsinVaccineRace,
    WisconsinVaccineSex,
)
from can_tools.scrapers.official.WY import WYCountyVaccinations, WYStateVaccinations
