""" RHEAS module for generating drought products.

.. module:: drought
   :synopsis: Module that contains functionality for generating drought products

.. moduleauthor:: Kostas Andreadis <kandread@jpl.nasa.gov>

"""

import numpy as np
import dbio
from dateutil.relativedelta import relativedelta
from scipy.stats import gamma, norm
from datetime import date, datetime, timedelta
import pandas


def _movingAverage(data, n):
    """Calculate the moving average from a time series."""
    out = np.cumsum(data)
    out[n:] = out[n:] - out[:-n]
    return out[n - 1:] / n


def calcSPI(duration, model, cid):
    """Calculate Standardized Precipitation Index for specified month
    *duration*. Need a climatology of precipitation stored in the database
    used in a VIC *model* simulation."""
    startdate = date(model.startyear + model.skipyear, model.startmonth, model.startday)
    enddate = date(model.endyear, model.endmonth, model.endday)
    nt = (enddate - startdate).days + 1
    ndays = ((startdate + relativedelta(months=duration)) - startdate).days + 1
    # tablename = "precip."+model.precip
    if duration < 1 or ndays > nt:
        print(
            "WARNING! Cannot calculate SPI with {0} months duration.".format(duration))
        spi = np.zeros(nt)
    else:
        p = np.loadtxt("{0}/forcings/data_{1:.{3}f}_{2:.{3}f}".format(model.model_path,
                                                                      model.gid[cid][0], model.gid[cid][1], model.grid_decimal))[:, 0]
        p = pandas.Series(p, [datetime(model.startyear, model.startmonth, model.startday) + timedelta(t) for t in range(len(p))])
        p[duration:] = pandas.rolling_mean(p.resample(
            'M', how='mean'), duration).values[duration:]
        p[:duration] = 0.0
        g1, g2, g3 = gamma.fit(p)
        cdf = gamma.cdf(p, g1, g2, g3)
        spi = norm.ppf(cdf)
    return spi


def calcSeverity(model, climatology, cid, varname="soil_moist"):
    """Calculate drought severity from *climatology* table stored in database."""
    nt = (date(model.endyear, model.endmonth, model.endday) -
          date(model.startyear + model.skipyear, model.startmonth, model.startday)).days + 1
    s = np.zeros(nt)
    return s


def calcDrySpells(model, cid, droughtfun=np.mean, duration=14, recovduration=2):
    """Calculate maps of number of dry spells during simulation period."""
    # FIXME: Currently only uses precipitation to identify dry spells. Need to change it to also use soil moisture and runoff
    p = np.loadtxt("{0}/forcings/data_{1:.{3}f}_{2:.{3}f}".format(model.model_path, model.gid[cid][0], model.gid[cid][1], model.grid_decimal))[:, 0]
    drought_thresh = droughtfun(p)
    ndroughts = np.zeros(len(p))
    days = 0
    for i in range(recovduration-1, len(p)):
        if p[i] <= drought_thresh:
            days += 1
        elif all(p[i-j] > drought_thresh for j in range(recovduration)):
            days = 0
        else:
            days += 1
        if days == duration:
            ndroughts[i] = 1
    return np.cumsum(ndroughts)


def calcPDSI(model, cid, prec, evap):
    """Calculate the Palmer Drought Severity Index."""
    # FIXME: Not functional at the moment
    dt = [date(model.startyear, model.startmonth, model.startday) +
          timedelta(t) for t in range(len(prec))]
    prec = pandas.Series(prec, dt).resample('M', how='mean')
    evap = pandas.Series(evap, dt).resample('M', how='mean')
    # Z = d * K
    pdsi = np.zeros(len(dt))
    # pdsi[1:] = 0.897 * pdsi[:-1] + 1.0 / 3.0 * Z
    return pdsi


def calc(varname, model, cid):
    """Calculate drought-related variable."""
    # nt = (date(model.endyear, model.endmonth, model.endday) -
    #       date(model.startyear + model.skipyear, model.startmonth, model.startday)).days + 1
    if varname.find("spi") == 0:
        duration = int(varname[3])
        output = calcSPI(duration, model, cid)
    elif varname == "severity":
        output = calcSeverity(model, "", cid)
    elif varname == "dryspells":
        output = calcDrySpells(model, cid)
    return output
