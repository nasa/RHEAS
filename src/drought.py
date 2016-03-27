""" RHEAS module for generating drought products.

.. module:: drought
   :synopsis: Module that contains functionality for generating drought products

.. moduleauthor:: Kostas Andreadis <kandread@jpl.nasa.gov>

"""

import numpy as np
from dateutil.relativedelta import relativedelta
import scipy.stats as stats
from datetime import date, datetime, timedelta
import pandas
import dbio


def _movingAverage(data, n):
    """Calculate the moving average from a time series."""
    out = np.cumsum(data)
    out[n:] = out[n:] - out[:-n]
    return out[n - 1:] / n


def calcSRI(duration, model, cid):
    """Calculate Standardized Runoff Index for specified month
    *duration*."""
    outvars = model.getOutputStruct(model.model_path + "/global.txt")
    startdate = date(model.startyear + model.skipyear, model.startmonth, model.startday)
    enddate = date(model.endyear, model.endmonth, model.endday)
    nt = (enddate - startdate).days + 1
    ndays = ((startdate + relativedelta(months=duration)) - startdate).days + 1
    if duration < 1 or ndays > nt:
        print(
            "WARNING! Cannot calculate SRI with {0} months duration.".format(duration))
        sri = np.zeros(nt)
    else:
        p = np.loadtxt("{0}/{1}_{2:.{4}f}_{3:.{4}f}".format(model.model_path, outvars['runoff'][0], model.gid[cid][0], model.gid[cid][1], model.grid_decimal))[:, outvars['runoff'][1]]
        p = pandas.Series(p, [datetime(model.startyear, model.startmonth, model.startday) + timedelta(t) for t in range(len(p))])
        pm = p.rolling(duration*30).mean()  # assume each month is 30 days
        g1, g2, g3 = stats.gamma.fit(pm[duration*30:])
        cdf = stats.gamma.cdf(pm, g1, g2, g3)
        sri = stats.norm.ppf(cdf)
        sri[np.isnan(sri)] = 0.0
        sri[np.isinf(sri)] = 0.0
    return sri


def calcSPI(duration, model, cid):
    """Calculate Standardized Precipitation Index for specified month
    *duration*."""
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
        pm = p.rolling(duration*30).mean()  # assume each month is 30 days
        g1, g2, g3 = stats.gamma.fit(pm[duration*30:])
        cdf = stats.gamma.cdf(pm, g1, g2, g3)
        spi = stats.norm.ppf(cdf)
        spi[np.isnan(spi)] = 0.0
        spi[np.isinf(spi)] = 0.0
    return spi


def calcSeverity(model, cid, varname="soil_moist"):
    """Calculate drought severity from *climatology* table stored in database."""
    outvars = model.getOutputStruct(model.model_path + "/global.txt")
    col = outvars[varname][1]
    if varname in ["soil_moist"]:
        p = np.loadtxt("{0}/{1}_{2:.{4}f}_{3:.{4}f}".format(model.model_path, outvars['runoff'][0], model.gid[cid][0], model.gid[cid][1], model.grid_decimal))[:, col:col+model.nlayers]
        p = pandas.Series(np.sum(p, axis=1), [datetime(model.startyear, model.startmonth, model.startday) + timedelta(t) for t in range(len(p))])
    else:
        p = np.loadtxt("{0}/{1}_{2:.{4}f}_{3:.{4}f}".format(model.model_path, outvars['runoff'][0], model.gid[cid][0], model.gid[cid][1], model.grid_decimal))[:, col]
        p = pandas.Series(p, [datetime(model.startyear, model.startmonth, model.startday) + timedelta(t) for t in range(len(p))])
    db = dbio.connect(model.dbname)
    cur = db.cursor()
    if dbio.tableExists(model.dbname, model.name, varname):
        if varname in ["soil_moist"]:
            lvar = ",layer"
        else:
            lvar = ""
        if dbio.columnExists(model.dbname, model.name, varname, "ensemble"):
            fsql = "with f as (select fdate{3},avg(st_value(rast,st_geomfromtext('POINT({0} {1})',4326))) as vals from {2}.{4} where st_intersects(rast,st_geomfromtext('POINT({0} {1})',4326)) group by fdate{3})".format(model.gid[cid][1], model.gid[cid][0], model.name, lvar, varname)
        else:
            fsql = "with f as (select fdate{3},st_value(rast,st_geomfromtext('POINT({0} {1})',4326)) as vals from {2}.{4} where st_intersects(rast,st_geomfromtext('POINT({0} {1})',4326)))".format(model.gid[cid][1], model.gid[cid][0], model.name, lvar, varname)
        sql = "{0} select fdate,sum(vals) from f group by fdate".format(fsql)
        cur.execute(sql)
        if bool(cur.rowcount):
            results = cur.fetchall()
            clim = pandas.Series([r[1] for r in results], [r[0] for r in results])
        else:
            clim = p
    else:
        print("WARNING! Climatology table does not exist. Severity calculation will be inaccurate!")
        clim = p
    s = 100.0 - np.array(map(lambda v: stats.percentileofscore(clim, v), p))
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


def calcSMDI(model, cid):
    """Calculate Soil Moisture Deficit Index (Narasimhan & Srinivasan, 2005)."""
    outvars = model.getOutputStruct(model.model_path + "/global.txt")
    col = outvars['soil_moist'][1]
    p = np.loadtxt("{0}/{1}_{2:.{4}f}_{3:.{4}f}".format(model.model_path, outvars['soil_moist'][0], model.gid[cid][0], model.gid[cid][1], model.grid_decimal))[:, col:col+model.nlayers]
    p = pandas.Series(np.sum(p, axis=1), [datetime(model.startyear, model.startmonth, model.startday) + timedelta(t) for t in range(len(p))])
    db = dbio.connect(model.dbname)
    cur = db.cursor()
    if dbio.tableExists(model.dbname, model.name, "soil_moist"):
        if dbio.columnExists(model.dbname, model.name, "soil_moist", "ensemble"):
            fsql = "with f as (select fdate,layer,avg(st_value(rast,st_geomfromtext('POINT({0} {1})',4326))) as sm from {2}.soil_moist where st_intersects(rast,st_geomfromtext('POINT({0} {1})',4326)) group by fdate,layer)".format(model.gid[cid][1], model.gid[cid][0], model.name)
        else:
            fsql = "with f as (select fdate,layer,st_value(rast,st_geomfromtext('POINT({0} {1})',4326)) as sm from {2}.soil_moist where st_intersects(rast,st_geomfromtext('POINT({0} {1})',4326)))".format(model.gid[cid][1], model.gid[cid][0], model.name)
        sql = "{0} select fdate,sum(sm) from f group by fdate".format(fsql)
        cur.execute(sql)
        if bool(cur.rowcount):
            results = cur.fetchall()
            clim = pandas.Series([r[1] for r in results], [r[0] for r in results])
        else:
            clim = p
    else:
        print("WARNING! Climatology table does not exist. SMDI calculation will be inaccurate!")
        clim = p
    smdi = np.zeros(len(p))
    MSW = clim.median()
    maxSW = clim.max()
    minSW = clim.min()
    for i in range(7, len(smdi)):
        SW = np.median(p[i-7:i+1])
        if SW == MSW:
            SD = (SW - MSW) / (MSW - minSW) * 100.0
        else:
            SD = (SW - MSW) / (maxSW - MSW) * 100.0
        if i > 7:
            smdi[i] = 0.5 * smdi[i-1] + SD / 50.0
        else:
            smdi[i] = SD / 50.0
    cur.close()
    db.close()
    return smdi


def calc(varname, model, cid):
    """Calculate drought-related variable."""
    # nt = (date(model.endyear, model.endmonth, model.endday) -
    #       date(model.startyear + model.skipyear, model.startmonth, model.startday)).days + 1
    if varname.find("spi") == 0:
        duration = int(varname[3])
        output = calcSPI(duration, model, cid)
    elif varname.startswith("sri"):
        duration = int(varname[3])
        output = calcSRI(duration, model, cid)
    elif varname == "severity":
        output = calcSeverity(model, cid)
    elif varname == "smdi":
        output = calcSMDI(model, cid)
    elif varname == "dryspells":
        output = calcDrySpells(model, cid)
    return output
