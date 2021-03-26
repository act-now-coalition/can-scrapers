from can_tools.scrapers import util
from can_tools.scrapers.base import CMU, DatasetBase
from can_tools.scrapers.ctp import (
    CovidTrackingProject,
    CovidTrackingProjectDemographics,
)
from can_tools.scrapers.official.AL.al_vaccine import ALCountyVaccine
from can_tools.scrapers.official.AZ.az_vaccine import (
    ArizonaVaccineCounty,
    ArizonaVaccineCountyAllocated,
)
from can_tools.scrapers.official.AZ.counties.maricopa_vaccine import (
    ArizonaMaricopaVaccine,
)
from can_tools.scrapers.official.CA.ca_vaccine import CaliforniaVaccineCounty
from can_tools.scrapers.official.CA.counties.la_vaccine import LACaliforniaCountyVaccine
from can_tools.scrapers.official.CA.counties.sandiego_vaccine import CASanDiegoVaccine
from can_tools.scrapers.official.CT.ct_vaccine import CTCountyVaccine
from can_tools.scrapers.official.DC.dc_cases import DCCases
from can_tools.scrapers.official.DC.dc_deaths import DCDeaths
from can_tools.scrapers.official.DC.dc_vaccines import DCVaccineSex
from can_tools.scrapers.official.DE.de_vaccine import DelawareStateVaccine
from can_tools.scrapers.official.federal.CDC.cdc_coviddatatracker import (
    CDCCovidDataTracker,
)
from can_tools.scrapers.official.federal.CDC.cdc_state_vaccines import CDCStateVaccine
from can_tools.scrapers.official.federal.CDC.cdc_variant_tracker import (
    CDCVariantTracker,
)
from can_tools.scrapers.official.federal.HHS.facility import (
    HHSReportedPatientImpactHospitalCapacityFacility,
)
from can_tools.scrapers.official.federal.HHS.hhs_state import (
    HHSReportedPatientImpactHospitalCapacityState,
)
from can_tools.scrapers.official.FL.fl_vaccine import FloridaCountyVaccine
from can_tools.scrapers.official.GA.ga_vaccines import GeorgiaCountyVaccine
from can_tools.scrapers.official.HI.hi_county import HawaiiVaccineCounty
# from can_tools.scrapers.official.IL import IllinoisDemographics, IllinoisHistorical
from can_tools.scrapers.official.IL.il_vaccine import (
    IllinoisVaccineCounty,
    IllinoisVaccineState,
)
from can_tools.scrapers.official.IN.in_vaccines import IndianaCountyVaccinations
from can_tools.scrapers.official.LA.la_county import LAVaccineCounty
from can_tools.scrapers.official.MD.md_vaccine import MarylandCountyVaccines
from can_tools.scrapers.official.ME.me_vaccines import MaineCountyVaccines
from can_tools.scrapers.official.MI.mi_vaccine import MichiganVaccineCounty
from can_tools.scrapers.official.MN.mn_vaccine import MinnesotaCountyVaccines
from can_tools.scrapers.official.MO.mo_vaccine import MissouriVaccineCounty
from can_tools.scrapers.official.MS.ms_vaccine import MSCountyVaccine
from can_tools.scrapers.official.MT.mt_vaccinations import (
    MontanaCountyVaccine,
    MontanaStateVaccine,
)

# from can_tools.scrapers.official.MA import Massachusetts
from can_tools.scrapers.official.NC.nc_vaccine import NCVaccine
from can_tools.scrapers.official.NJ.nj_vaccine import NewJerseyVaccineCounty
from can_tools.scrapers.official.NM.nm_vaccine import NewMexicoVaccineCounty
from can_tools.scrapers.official.NV.nv_vaccines import NevadaCountyVaccines
from can_tools.scrapers.official.NY.ny_vaccine import NewYorkVaccineCounty
from can_tools.scrapers.official.OH.oh_vaccine import OhioVaccineCounty
from can_tools.scrapers.official.OR.or_vaccine import OregonVaccineCounty
from can_tools.scrapers.official.PA.pa_vaccines import (
    PennsylvaniaCountyVaccines,
    PennsylvaniaVaccineAge,
    PennsylvaniaVaccineEthnicity,
    PennsylvaniaVaccineRace,
    PennsylvaniaVaccineSex,
)
from can_tools.scrapers.official.TN.tn_state import (
    TennesseeAge,
    TennesseeAgeByCounty,
    TennesseeRaceEthnicitySex,
)
from can_tools.scrapers.official.TN.tn_vaccine import TennesseeVaccineCounty
from can_tools.scrapers.official.TX.texas_vaccine import (
    TexasCountyVaccine,
    TexasStateVaccine,
    TXVaccineCountyAge,
    TXVaccineCountyRace,
)
from can_tools.scrapers.official.VA.va_vaccine import VirginiaVaccine
from can_tools.scrapers.official.VT.vt_vaccinations import (
    VermontCountyVaccine,
    VermontStateVaccine,
)
from can_tools.scrapers.official.WA.wa_vaccine import WashingtonVaccine
from can_tools.scrapers.official.WI.wi_county_vaccine import WisconsinVaccineCounty
from can_tools.scrapers.official.WI.wi_demographic_vaccine import (
    WisconsinVaccineAge,
    WisconsinVaccineEthnicity,
    WisconsinVaccineRace,
    WisconsinVaccineSex,
)
from can_tools.scrapers.official.WI.wi_state import WisconsinCounties, WisconsinState
from can_tools.scrapers.official.WV.wv_vaccine import WVCountyVaccine
from can_tools.scrapers.official.WY.WYVaccines import (
    WYCountyVaccinations,
    WYStateVaccinations,
)
from can_tools.scrapers.usafacts import USAFactsCases, USAFactsDeaths
