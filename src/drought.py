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
import logging


def _clipToValidRange(data):
    """Clip data series to valid intervals for drought index values."""
    valid_min = -3.09
    valid_max = 3.09
    return np.clip(data, valid_min, valid_max)


def _movingAverage(data, n):
    """Calculate the moving average from a time series."""
    out = np.cumsum(data)
    out[n:] = out[n:] - out[:-n]
    return out[n - 1:] / n


def _calcSuctionHead(model, cid, nlayers=3):
    """Calculate soil suction from soil moisture using the Clapp
    and Hornberger (1978) model and parameters."""
    Ksat = np.array([63.36, 56.16, 12.49, 2.59, 2.5, 2.27, 0.612, 0.882, 0.781, 0.371, 0.461])
    Ksat /= (10 * 24.)  # convert from cm/hr to mm/day
    n = [.395, .41, .435, .485, .451, .42, .477, .476, .426, .492, .482]
    psi_a = [121., 90., 218., 786., 478., 299., 356., 630., 153., 490., 405.]
    b = [4.05, 4.38, 4.9, 5.3, 5.39, 7.12, 7.75, 8.52, 10.4, 10.4, 11.4]
    # get saturated conductivity values to identify soil type
    db = dbio.connect(model.dbname)
    cur = db.cursor()
    cur.execute("select line from vic.soils where id={0}".format(cid))
    line = cur.fetchone()[0]
    k = np.mean(np.array(map(float, line.split()[9+nlayers:nlayers+11])))
    z = sum(map(float, line.split()[4*nlayers+10:4*nlayers+12])) * 1000.
    ki = np.argmin(abs(Ksat - k))
    # get soil moisture for surface and root zone layer
    sql = "select fdate,sum(st_value(rast,geom)) from {0}.soil_moist, {0}.basin where st_intersects(rast,geom) and gid={1} and layer<3 group by fdate order by fdate".format(model.name, cid)
    cur.execute(sql)
    if bool(cur.rowcount):
        results = cur.fetchall()
        sm = pandas.Series([r[1] for r in results], np.array([r[0] for r in results], dtype='datetime64'))
        # convert into dekad averages
        d = sm.index.day - np.clip((sm.index.day-1) // 10, 0, 2)*10 - 1
        date = sm.index.values - np.array(d, dtype='timedelta64[D]')
        sm = sm.groupby(date).mean()
        # calculate soil suction
        pf = np.log(psi_a[ki] * ((sm / z) / n[ki])**(-b[ki]))
        # calculate z-score of soil suction
        st = "{0}-{1}-{2}".format(model.startyear, model.startmonth, model.startday)
        et = "{0}-{1}-{2}".format(model.endyear, model.endmonth, model.endday)
        pfz = (pf[st:et] - pf.mean()) / pf.std()
        pfz = pfz.resample('D').ffill().values
    else:
        pfz = None
    cur.close()
    db.close()
    return pfz


def _calcFpar(model, cid):
    """Retrieve the Photosynthetically Active Radiation from the model simulation."""
    db = dbio.connect(model.dbname)
    cur = db.cursor()
    sql = "select fdate,st_value(rast,geom) from {0}.par, {0}.basin where st_intersects(rast,geom) and gid={1} order by fdate".format(model.name, cid)
    cur.execute(sql)
    if bool(cur.rowcount):
        results = cur.fetchall()
        fpar = pandas.Series([r[1] for r in results], np.array([r[0] for r in results], dtype='datetime64'))
        d = fpar.index.day - np.clip((fpar.index.day-1) // 10, 0, 2)*10 - 1
        date = fpar.index.values - np.array(d, dtype='timedelta64[D]')
        fpar = fpar.groupby(date).mean()
        st = "{0}-{1}-{2}".format(model.startyear, model.startmonth, model.startday)
        et = "{0}-{1}-{2}".format(model.endyear, model.endmonth, model.endday)
        fparz = (fpar[st:et] - fpar.mean()) / fpar.std()
        fparz = fparz.resample('D').ffill().values
    else:
        fparz = None
    cur.close()
    db.close()
    return fparz


def calcCDI(model, cid):
    """Calculate Combined Drought Index as a monthly time series. The index is
    categorical with the values corresponding to:
    0 = No drought
    1 = Watch (Precipitation deficit)
    2 = Warning (Soil moisture deficit)
    3 = Alert 1 (Vegetation stress following precipitation deficit)
    4 = Alert 2 (Vegetation stress following precipitation/soil moisture deficit)."""
    spi = calcSPI(3, model, cid)
    sma = _calcSuctionHead(model, cid)
    fapar = _calcFpar(model, cid)
    cdi = np.zeros(len(spi), dtype='int')
    cdi[spi < -1] = 1
    cdi[fapar > 1 & spi < -1] = 2
    cdi[fapar < -1 & spi < -1] = 3
    cdi[fapar < -1 & sma > 1 & spi < -1] = 4
    return cdi


def calcSRI(duration, model, cid):
    """Calculate Standardized Runoff Index for specified month
    *duration*."""
    log = logging.getLogger(__name__)
    # outvars = model.getOutputStruct(model.model_path + "/global.txt")
    startdate = date(model.startyear + model.skipyear, model.startmonth, model.startday)
    enddate = date(model.endyear, model.endmonth, model.endday)
    nt = (enddate - startdate).days + 1
    ndays = ((startdate + relativedelta(months=duration)) - startdate).days + 1
    if duration < 1 or ndays > nt:
        log.warning("Cannot calculate SRI with {0} months duration.".format(duration))
        sri = np.zeros(nt)
    else:
        db = dbio.connect(model.dbname)
        cur = db.cursor()
        sql = "select fdate,st_value(rast,geom) from {0}.runoff, {0}.basin where st_intersects(rast,geom) and gid={1} and fdate>=date'{2}-{3}-{4} and fdate<=date'{5}-{6}-{7} order by fdate"
        cur.execute(sql)
        results = cur.fetchall()
        p = pandas.Series([r[1] for r in results], np.array([r[0] for r in results], dtype='datetime64'))
        # p = np.loadtxt("{0}/{1}_{2:.{4}f}_{3:.{4}f}".format(model.model_path, outvars['runoff'][0], model.gid[cid][0], model.gid[cid][1], model.grid_decimal))[:, outvars['runoff'][1]]
        # p = pandas.Series(p, [datetime(model.startyear, model.startmonth, model.startday) + timedelta(t) for t in range(len(p))])
        pm = p.rolling(duration*30).mean()  # assume each month is 30 days
        g1, g2, g3 = stats.gamma.fit(pm[duration*30:])
        cdf = stats.gamma.cdf(pm, g1, g2, g3)
        sri = stats.norm.ppf(cdf)
        sri[np.isnan(sri)] = 0.0
        sri[np.isinf(sri)] = 0.0
        sri = _clipToValidRange(sri)
        cur.close()
        db.close()
    return sri


def calcSPI(duration, model, cid):
    """Calculate Standardized Precipitation Index for specified month
    *duration*."""
    log = logging.getLogger(__name__)
    startdate = date(model.startyear + model.skipyear, model.startmonth, model.startday)
    enddate = date(model.endyear, model.endmonth, model.endday)
    nt = (enddate - startdate).days + 1
    ndays = ((startdate + relativedelta(months=duration)) - startdate).days + 1
    # tablename = "precip."+model.precip
    if duration < 1 or ndays > nt:
        log.warning("Cannot calculate SPI with {0} months duration.".format(duration))
        spi = np.zeros(nt)
    else:
        db = dbio.connect(model.dbname)
        cur = db.cursor()
        sql = "select fdate,st_value(rast,geom) from {0}.rainf, {0}.basin where st_intersects(rast,geom) and gid={1} and fdate>=date'{2}-{3}-{4} and fdate<=date'{5}-{6}-{7} order by fdate".format(model.name, cid, model.startyear, model.startmonth, model.startday, model.endyear, model.endmonth, model.endday)
        cur.execute(sql)
        results = cur.fetchall()
        p = pandas.Series([r[1] for r in results], np.array([r[0] for r in results], dtype='datetime64'))
        # p = np.loadtxt("{0}/forcings/data_{1:.{3}f}_{2:.{3}f}".format(model.model_path,
        #                                                               model.gid[cid][0], model.gid[cid][1], model.grid_decimal))[:, 0]
        # p = pandas.Series(p, [datetime(model.startyear, model.startmonth, model.startday) + timedelta(t) for t in range(len(p))])
        pm = p.rolling(duration*30).mean()  # assume each month is 30 days
        g1, g2, g3 = stats.gamma.fit(pm[duration*30:])
        cdf = stats.gamma.cdf(pm, g1, g2, g3)
        spi = stats.norm.ppf(cdf)
        spi[np.isnan(spi)] = 0.0
        spi[np.isinf(spi)] = 0.0
        spi = _clipToValidRange(spi)
        cur.close()
        db.close()
    return spi


def calcSeverity(model, cid, varname="soil_moist"):
    """Calculate drought severity from *climatology* table stored in database."""
    # log = logging.getLogger(__name__)
    db = dbio.connect(model.dbname)
    cur = db.cursor()
    if varname == "soil_moist":
        sql = "select fdate,sum(st_value(rast,geom)) from {0}.soil_moist, {0}.basin where st_intersects(rast,geom) and gid={1} group by fdate order by fdate".format(model.name, cid)
    else:
        sql = "select fdate,st_value(rast,geom) from {0}.runoff, {0}.basin where st_intersects(rast,geom) and gid={1} order by fdate".format(model.name, cid)
    cur.execute(sql)
    results = cur.fetchall()
    p = pandas.Series([r[1] for r in results], np.array([r[0] for r in results], dtype='datetime64'))
    st = "{0}-{1}-{2}".format(model.startyear, model.startmonth, model.startday)
    et = "{0}-{1}-{2}".format(model.endyear, model.endmonth, model.endday)
    s = 100.0 - np.array([stats.percentileofscore(p.values, v) for v in p[st:et]])
    cur.close()
    db.close()
    # outvars = model.getOutputStruct(model.model_path + "/global.txt")
    # col = outvars[varname][1]
    # if varname in ["soil_moist"]:
    #     p = np.loadtxt("{0}/{1}_{2:.{4}f}_{3:.{4}f}".format(model.model_path, outvars['runoff'][0], model.gid[cid][0], model.gid[cid][1], model.grid_decimal))[:, col:col+model.nlayers]
    #     p = pandas.Series(np.sum(p, axis=1), [datetime(model.startyear, model.startmonth, model.startday) + timedelta(t) for t in range(len(p))])
    # else:
    #     p = np.loadtxt("{0}/{1}_{2:.{4}f}_{3:.{4}f}".format(model.model_path, outvars['runoff'][0], model.gid[cid][0], model.gid[cid][1], model.grid_decimal))[:, col]
    #     p = pandas.Series(p, [datetime(model.startyear, model.startmonth, model.startday) + timedelta(t) for t in range(len(p))])
    # db = dbio.connect(model.dbname)
    # cur = db.cursor()
    # if dbio.tableExists(model.dbname, model.name, varname):
    #     if varname in ["soil_moist"]:
    #         lvar = ",layer"
    #     else:
    #         lvar = ""
    #     if dbio.columnExists(model.dbname, model.name, varname, "ensemble"):
    #         fsql = "with f as (select fdate{3},avg(st_value(rast,st_geomfromtext('POINT({0} {1})',4326))) as vals from {2}.{4} where st_intersects(rast,st_geomfromtext('POINT({0} {1})',4326)) group by fdate{3})".format(model.gid[cid][1], model.gid[cid][0], model.name, lvar, varname)
    #     else:
    #         fsql = "with f as (select fdate{3},st_value(rast,st_geomfromtext('POINT({0} {1})',4326)) as vals from {2}.{4} where st_intersects(rast,st_geomfromtext('POINT({0} {1})',4326)))".format(model.gid[cid][1], model.gid[cid][0], model.name, lvar, varname)
    #     sql = "{0} select fdate,sum(vals) from f group by fdate".format(fsql)
    #     cur.execute(sql)
    #     if bool(cur.rowcount):
    #         results = cur.fetchall()
    #         clim = pandas.Series([r[1] for r in results], [r[0] for r in results])
    #     else:
    #         clim = p
    # else:
    #     log.warning("Climatology table does not exist. Severity calculation will be inaccurate!")
    #     clim = p
    # s = 100.0 - np.array(map(lambda v: stats.percentileofscore(clim, v), p))
    return s


def calcDrySpells(model, cid, droughtfun=np.mean, duration=14, recovduration=2):
    """Calculate maps of number of dry spells during simulation period."""
    # FIXME: Currently only uses precipitation to identify dry spells. Need to change it to also use soil moisture and runoff
    db = dbio.connect(model.dbname)
    cur = db.cursor()
    sql = "select fdate,st_value(rast,geom) from {0}.rainf, {0}.basin where st_intersects(rast,geom) and gid={1} and fdate>=date'{2}-{3}-{4} and fdate<=date'{5}-{6}-{7} order by fdate".format(model.name, cid, model.startyear, model.startmonth, model.startday, model.endyear, model.endmonth, model.endday)
    cur.execute(sql)
    results = cur.fetchall()
    p = pandas.Series([r[1] for r in results], np.array([r[0] for r in results], dtype='datetime64'))
    cur.close()
    db.close()
    # p = np.loadtxt("{0}/forcings/data_{1:.{3}f}_{2:.{3}f}".format(model.model_path, model.gid[cid][0], model.gid[cid][1], model.grid_decimal))[:, 0]
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


def calcSMDI(model, cid):
    """Calculate Soil Moisture Deficit Index (Narasimhan & Srinivasan, 2005)."""
    # log = logging.getLogger(__name__)
    db = dbio.connect(model.dbname)
    cur = db.cursor()
    sql = "select fdate,st_value(rast,geom) from {0}.soil_moist, {0}.basin where st_intersects(rast,geom) and layer=2 and gid={1} group by fdate order by fdate".format(model.name, cid)
    cur.execute(sql)
    results = cur.fetchall()
    clim = pandas.Series([r[1] for r in results], np.array([r[0] for r in results], dtype='datetime64'))
    st = "{0}-{1}-{2}".format(model.startyear, model.startmonth, model.startday)
    et = "{0}-{1}-{2}".format(model.endyear, model.endmonth, model.endday)
    p = clim[st:et]
    # outvars = model.getOutputStruct(model.model_path + "/global.txt")
    # col = outvars['soil_moist'][1]
    # p = np.loadtxt("{0}/{1}_{2:.{4}f}_{3:.{4}f}".format(model.model_path, outvars['soil_moist'][0], model.gid[cid][0], model.gid[cid][1], model.grid_decimal))[:, col:col+model.nlayers]
    # p = pandas.Series(np.sum(p, axis=1), [datetime(model.startyear, model.startmonth, model.startday) + timedelta(t) for t in range(len(p))])
    # db = dbio.connect(model.dbname)
    # cur = db.cursor()
    # if dbio.tableExists(model.dbname, model.name, "soil_moist"):
    #     if dbio.columnExists(model.dbname, model.name, "soil_moist", "ensemble"):
    #         fsql = "with f as (select fdate,layer,avg(st_value(rast,st_geomfromtext('POINT({0} {1})',4326))) as sm from {2}.soil_moist where st_intersects(rast,st_geomfromtext('POINT({0} {1})',4326)) group by fdate,layer)".format(model.gid[cid][1], model.gid[cid][0], model.name)
    #     else:
    #         fsql = "with f as (select fdate,layer,st_value(rast,st_geomfromtext('POINT({0} {1})',4326)) as sm from {2}.soil_moist where st_intersects(rast,st_geomfromtext('POINT({0} {1})',4326)))".format(model.gid[cid][1], model.gid[cid][0], model.name)
    #     sql = "{0} select fdate,sum(sm) from f group by fdate".format(fsql)
    #     cur.execute(sql)
    #     if bool(cur.rowcount):
    #         results = cur.fetchall()
    #         clim = pandas.Series([r[1] for r in results], [r[0] for r in results])
    #     else:
    #         clim = p
    # else:
    #     log.warning("Climatology table does not exist. SMDI calculation will be inaccurate!")
    #     clim = p
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
    elif varname == "cdi":
        output = calcCDI(model, cid)
    elif varname == "smdi":
        output = calcSMDI(model, cid)
    elif varname == "dryspells":
        output = calcDrySpells(model, cid)
    return output
