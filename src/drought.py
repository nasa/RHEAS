""" RHEAS module for generating drought products.

.. module:: drought
   :synopsis: Module that contains functionality for generating drought products

.. moduleauthor:: Kostas Andreadis <kandread@jpl.nasa.gov>

"""

import numpy as np
from dateutil.relativedelta import relativedelta
import scipy.stats as stats
from datetime import date
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


def _calcSuctionHead(model, nlayers=3):
    """Calculate soil suction from soil moisture using the Clapp
    and Hornberger (1978) model and parameters."""
    Ksat = np.array([63.36, 56.16, 12.49, 2.59, 2.5, 2.27, 0.612, 0.882, 0.781, 0.371, 0.461])
    Ksat *= (10 * 24.)  # convert from cm/hr to mm/day
    n = [.395, .41, .435, .485, .451, .42, .477, .476, .426, .492, .482]
    psi_a = [121., 90., 218., 786., 478., 299., 356., 630., 153., 490., 405.]
    b = [4.05, 4.38, 4.9, 5.3, 5.39, 7.12, 7.75, 8.52, 10.4, 10.4, 11.4]
    db = dbio.connect(model.dbname)
    cur = db.cursor()
    # get georeference information
    cur.execute("select st_upperleftx(rast), st_upperlefty(rast), st_scalex(rast), st_scaley(rast) from {0}.soil_moist".format(model.name))
    results = cur.fetchone()
    ulx, uly, xres, yres = results
    # get soil moisture for surface and root zone layer
    sql = "select fdate,(ST_DumpValues(st_union(rast,'sum'))).valarray from {0}.soil_moist where layer<3 group by fdate order by fdate".format(model.name)
    cur.execute(sql)
    if bool(cur.rowcount):
        st = "{0}-{1}-{2}".format(model.startyear, model.startmonth, model.startday)
        et = "{0}-{1}-{2}".format(model.endyear, model.endmonth, model.endday)
        results = cur.fetchall()
        data = np.array([np.array(r[1]).ravel() for r in results])
        i = np.where(np.not_equal(data[0, :], None))[0]
        sm = pandas.DataFrame(data[:, i], index=np.array([r[0] for r in results], dtype='datetime64'), columns=range(len(i)))
        pfz = np.zeros(sm[st:et].shape)
        ii, jj = np.unravel_index(i, np.array(results[0][1]).shape)
        for j in sm.columns:
            # identify soil type by saturated conductivity
            cur.execute("select line from vic.soils order by geom <-> st_geomfromtext('POINT({0} {1})', 4326) limit 1".format(ulx+xres*jj[j], uly+yres*ii[j]))
            line = cur.fetchone()[0]
            k = np.mean(np.array(map(float, line.split()[9+nlayers:nlayers+11])))
            z = sum(map(float, line.split()[4*nlayers+10:4*nlayers+12])) * 1000.
            ki = np.argmin(abs(Ksat - k))
            # convert into dekad averages
            d = sm[j].index.day - np.clip((sm[j].index.day-1) // 10, 0, 2)*10 - 1
            date = sm[j].index.values - np.array(d, dtype='timedelta64[D]')
            sm_dekad = sm[j].groupby(date).apply(np.mean)
            # calculate soil suction
            pf = np.log(psi_a[ki] * ((sm_dekad / z) / n[ki])**(-b[ki]))
            # calculate z-score of soil suction
            pf = (pf[st:et] - pf.mean()) / pf.std()
            pfz[:, j] = pf.reindex(sm[st:et].index).ffill().values
    else:
        pfz = None
    cur.close()
    db.close()
    return pfz


def _calcFpar(model):
    """Retrieve the Photosynthetically Active Radiation from the model simulation."""
    db = dbio.connect(model.dbname)
    cur = db.cursor()
    sql = "select fdate,(ST_DumpValues(rast)).valarray from {0}.par order by fdate".format(model.name)
    cur.execute(sql)
    if bool(cur.rowcount):
        results = cur.fetchall()
        data = np.array([np.array(r[1]).ravel() for r in results])
        i = np.where(np.not_equal(data[0, :], None))[0]
        fpar = pandas.DataFrame(data[:, i], index=np.array([r[0] for r in results], dtype='datetime64'), columns=range(len(i)))
        d = fpar.index.day - np.clip((fpar.index.day-1) // 10, 0, 2)*10 - 1
        date = fpar.index.values - np.array(d, dtype='timedelta64[D]')
        fpar_dekad = fpar.groupby(date, axis=0).apply(np.mean)
        st = "{0}-{1}-{2}".format(model.startyear, model.startmonth, model.startday)
        et = "{0}-{1}-{2}".format(model.endyear, model.endmonth, model.endday)
        fparz = (fpar_dekad[st:et] - fpar_dekad.mean(axis=0)) / fpar_dekad.std(axis=0)
        fparz = fparz.reindex(fpar[st:et].index).ffill().values
    else:
        fparz = None
    cur.close()
    db.close()
    return fparz


def calcCDI(model):
    """Calculate Combined Drought Index as a monthly time series. The index is
    categorical with the values corresponding to:
    0 = No drought
    1 = Watch (Precipitation deficit)
    2 = Warning (Soil moisture deficit)
    3 = Alert 1 (Vegetation stress following precipitation deficit)
    4 = Alert 2 (Vegetation stress following precipitation/soil moisture deficit)."""
    spi = calcSPI(3, model)
    sma = _calcSuctionHead(model)
    fapar = _calcFpar(model)
    if all(v is not None for v in [spi, sma, fapar]):
        cdi = np.zeros(spi.shape, dtype='int')
        cdi[spi < -1] = 1
        cdi[(fapar > 1) & (spi < -1)] = 2
        cdi[(fapar < -1) & (spi < -1)] = 3
        cdi[(fapar < -1) & (sma > 1) & (spi < -1)] = 4
    else:
        cdi = None
    return cdi


def calcSRI(duration, model):
    """Calculate Standardized Runoff Index for specified month
    *duration*."""
    log = logging.getLogger(__name__)
    startdate = date(model.startyear + model.skipyear, model.startmonth, model.startday)
    enddate = date(model.endyear, model.endmonth, model.endday)
    nt = (enddate - startdate).days + 1
    ndays = ((startdate + relativedelta(months=duration)) - startdate).days + 1
    if duration < 1 or ndays > nt:
        log.warning("Cannot calculate SRI with {0} months duration.".format(duration))
        sri = None
    else:
        db = dbio.connect(model.dbname)
        cur = db.cursor()
        sql = "select fdate,(ST_DumpValues(rast)).valarray from {0}.runoff where fdate>=date'{1}-{2}-{3}' and fdate<=date'{4}-{5}-{6}' order by fdate".format(model.name, model.startyear, model.startmonth, model.startday, model.endyear, model.endmonth, model.endday)
        cur.execute(sql)
        results = cur.fetchall()
        data = np.array([np.array(r[1]).ravel() for r in results])
        i = np.where(np.not_equal(data[0, :], None))[0]
        p = pandas.DataFrame(data[:, i], index=np.array([r[0] for r in results], dtype='datetime64'), columns=range(len(i)))
        pm = p.rolling(duration*30).mean()  # assume each month is 30 days
        g = [stats.gamma.fit(pm[j][duration*30:]) for j in pm.columns]
        cdf = np.array([stats.gamma.cdf(pm[j],*g[j]) for j in pm.columns]).T
        sri = np.zeros(cdf.shape)
        sri[duration*30:, :] = stats.norm.ppf(cdf[duration*30:, :])
        sri = _clipToValidRange(sri)
        cur.close()
        db.close()
    return sri


def calcSPI(duration, model):
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
        spi = None
    else:
        db = dbio.connect(model.dbname)
        cur = db.cursor()
        sql = "select fdate,(ST_DumpValues(rast)).valarray from {0}.rainf where fdate>=date'{1}-{2}-{3}' and fdate<=date'{4}-{5}-{6}' order by fdate".format(model.name, model.startyear, model.startmonth, model.startday, model.endyear, model.endmonth, model.endday)
        cur.execute(sql)
        results = cur.fetchall()
        data = np.array([np.array(r[1]).ravel() for r in results])
        i = np.where(np.not_equal(data[0, :], None))[0]
        p = pandas.DataFrame(data[:, i], index=np.array([r[0] for r in results], dtype='datetime64'), columns=range(len(i)))
        pm = p.rolling(duration*30).mean()  # assume each month is 30 days
        g = [stats.gamma.fit(pm[j][duration*30:]) for j in pm.columns]
        cdf = np.array([stats.gamma.cdf(pm[j],*g[j]) for j in pm.columns]).T
        spi = np.zeros(cdf.shape)
        spi[duration*30:, :] = stats.norm.ppf(cdf[duration*30:, :])
        spi = _clipToValidRange(spi)
        cur.close()
        db.close()
    return spi


def calcSeverity(model, varname="soil_moist"):
    """Calculate drought severity from *climatology* table stored in database."""
    db = dbio.connect(model.dbname)
    cur = db.cursor()
    if varname == "soil_moist":
        sql = "select fdate,(ST_DumpValues(st_union(rast,'sum'))).valarray from {0}.soil_moist group by fdate order by fdate".format(model.name)
    else:
        sql = "select fdate,(ST_DumpValues(rast)).valarray from {0}.runoff order by fdate".format(model.name)
    cur.execute(sql)
    results = cur.fetchall()
    data = np.array([np.array(r[1]).ravel() for r in results])
    i = np.where(np.not_equal(data[0, :], None))[0]
    p = pandas.DataFrame(data[:, i], index=np.array([r[0] for r in results], dtype='datetime64'), columns=range(len(i)))
    p = p.rolling('10D').mean()  # calculate percentiles with dekad rolling mean
    st = "{0}-{1}-{2}".format(model.startyear, model.startmonth, model.startday)
    et = "{0}-{1}-{2}".format(model.endyear, model.endmonth, model.endday)
    s = np.array([[stats.percentileofscore(p[pi].values, v) for v in p[pi][st:et]] for pi in p.columns]).T
    s = 100.0 - s
    cur.close()
    db.close()
    return s


def calcDrySpells(model, droughtfun=np.mean, duration=14, recovduration=2):
    """Calculate maps of number of dry spells during simulation period."""
    # FIXME: Currently only uses precipitation to identify dry spells. Need to change it to also use soil moisture and runoff
    db = dbio.connect(model.dbname)
    cur = db.cursor()
    sql = "select fdate,(ST_DumpValues(rast)).valarray from {0}.rainf where fdate>=date'{1}-{2}-{3}' and fdate<=date'{4}-{5}-{6}' order by fdate".format(model.name, model.startyear, model.startmonth, model.startday, model.endyear, model.endmonth, model.endday)
    cur.execute(sql)
    results = cur.fetchall()
    data = np.array([np.array(r[1]).ravel() for r in results])
    i = np.where(np.not_equal(data[0, :], None))[0]
    p = pandas.DataFrame(data[:, i], index=np.array([r[0] for r in results], dtype='datetime64'), columns=range(len(i)))
    cur.close()
    db.close()
    ndroughts = np.zeros(p.values.shape)
    for pi in p.columns:
        drought_thresh = droughtfun(p[pi])
        days = 0
        for i in range(recovduration-1, len(p[pi])):
            if p.values[i, pi] <= drought_thresh:
                days += 1
            elif all(p.values[i-j, pi] > drought_thresh for j in range(recovduration)):
                days = 0
            else:
                days += 1
            if days == duration:
                ndroughts[i, pi] = 1
    return np.cumsum(ndroughts, axis=0)


def calcSMDI(model):
    """Calculate Soil Moisture Deficit Index (Narasimhan & Srinivasan, 2005)."""
    db = dbio.connect(model.dbname)
    cur = db.cursor()
    sql = "select fdate,(ST_DumpValues(rast)).valarray from {0}.soil_moist where layer=2 order by fdate".format(model.name)
    cur.execute(sql)
    results = cur.fetchall()
    data = np.array([np.array(r[1]).ravel() for r in results])
    i = np.where(np.not_equal(data[0, :], None))[0]
    clim = pandas.DataFrame(data[:, i], index=np.array([r[0] for r in results], dtype='datetime64'), columns=range(len(i)))
    st = "{0}-{1}-{2}".format(model.startyear, model.startmonth, model.startday)
    et = "{0}-{1}-{2}".format(model.endyear, model.endmonth, model.endday)
    p = clim[st:et]
    smdi = np.zeros(p.shape)
    for j in clim.columns:
        MSW = clim[j].median()
        maxSW = clim[j].max()
        minSW = clim[j].min()
        SW = p[j].rolling('7D').median().values[7:]
        SD = (SW - MSW) / (maxSW - MSW) * 100.0
        SD[SD == 0.0] = (SW[SD == 0.0] - MSW) / (MSW - minSW) * 100.0
        smdi[:7, j] = SD[:7] / 50.0
        smdi[7:, j] = 0.5 * smdi[6:-1, j] + SD / 50.0
    cur.close()
    db.close()
    smdi = np.clip(smdi, -4.0, 4.0)
    return smdi


def calc(varname, model):
    """Calculate drought-related variable."""
    if varname.find("spi") == 0:
        duration = int(varname[3])
        output = calcSPI(duration, model)
    elif varname.startswith("sri"):
        duration = int(varname[3])
        output = calcSRI(duration, model)
    elif varname == "severity":
        output = calcSeverity(model)
    elif varname == "cdi":
        output = calcCDI(model)
    elif varname == "smdi":
        output = calcSMDI(model)
    elif varname == "dryspells":
        output = calcDrySpells(model)
    return output
