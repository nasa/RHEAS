""" RHEAS module for retrieving meteorological forecasts/hindcasts
from the IRI FD Seasonal Forecast Tercile Probabilities.

.. module:: iri
   :synopsis: Retrieve IRI meteorological forecast data

.. moduleauthor:: Kostas Andreadis <kandread@jpl.nasa.gov>

"""

import datasets
import dbio
import netCDF4 as netcdf
import os
import random
import string
import numpy as np
from datetime import date
from dateutil.relativedelta import relativedelta


def dates(dbname):
    dts = datasets.dates(dbname, "precip.iri")
    return dts


def ingest(dbname, filename, dt, lt, cname, stname):
    """Imports Geotif *filename* into database *db*."""
    db = dbio.connect(dbname)
    cur = db.cursor()
    schemaname, tablename = stname.split(".")
    cur.execute(
        "select * from information_schema.tables where table_schema='{0}' and table_name='{1}'".format(schemaname, tablename))
    if not bool(cur.rowcount):
        cur.execute("create table {0}.{1} (rid serial not null primary key, fdate date, tercile text, leadtime int, rast raster)".format(
            schemaname, tablename))
        db.commit()
    cur.execute("select * from {0} where fdate='{1}' and tercile = '{2}' and leadtime = {3}".format(stname, dt.strftime("%Y-%m-%d"), cname, lt))
    if bool(cur.rowcount):
        cur.execute("delete from {0} where fdate='{1}' and tercile = '{2}' and leadtime = {3}".format(stname, dt.strftime("%Y-%m-%d"), cname, lt))
        db.commit()
    dbio.ingest(dbname, filename, dt, stname, False, False)
    sql = "update {0} set tercile = '{1}' where tercile is null".format(
        stname, cname)
    cur.execute(sql)
    sql = "update {0} set leadtime = '{1}' where leadtime is null".format(
        stname, lt)
    cur.execute(sql)
    db.commit()
    cur.close()


def download(dbname, dts, bbox=None):
    """Downloads IRI forecast tercile probability data from the IRI data server,
    and imports them into the database *db*. Optionally uses a bounding box to
    limit the region with [minlon, minlat, maxlon, maxlat]."""
    leadtime = 3
    res = 2.5
    baseurl = "http://iridl.ldeo.columbia.edu/SOURCES/.IRI/.FD/.Seasonal_Forecast/.{0}/.prob/dods"
    table = {"Precipitation": "precip.iri", "Temperature": "tmax.iri"}
    for varname in ["Precipitation", "Temperature"]:
        purl = baseurl.format(varname)
        pds = netcdf.Dataset(purl)
        lat = pds.variables["Y"][:]
        lon = pds.variables["X"][:]
        lon[lon > 180] -= 360.0
        i1, i2, j1, j2 = datasets.spatialSubset(np.sort(lat)[::-1], np.sort(lon), res, bbox)
        lati = np.argsort(lat)[::-1][i1:i2]
        loni = np.argsort(lon)[j1:j2]
        lat = np.sort(lat)[::-1][i1:i2]
        lon = np.sort(lon)[j1:j2]
        t = pds.variables["F"][:]
        ti = [tt for tt in range(len(t)) if t[tt] >= ((dts[0].year - 1960) * 12 + dts[0].month - 0.5) and t[tt] <= ((dts[1].year - 1960) * 12 + dts[1].month - 0.5)]
        for tt in ti:
            dt = date(1960, 1, 1) + relativedelta(months=int(t[tt]))
            for m in range(leadtime):
                for ci, c in enumerate(["below", "normal", "above"]):
                    data = pds.variables["prob"][tt, m, lati, loni, ci]
                    filename = dbio.writeGeotif(lat, lon, res, data)
                    ingest(dbname, filename, dt, m + 1, c, table[varname])
                    os.remove(filename)


def _getResampledTables(dbname, options, res):
    """Find names of resampled raster tables."""
    rtables = {}
    db = dbio.connect(dbname)
    cur = db.cursor()
    for v in ['precip', 'tmax', 'tmin', 'wind']:
        tname = options['vic'][v]
        cur.execute(
            "select * from raster_resampled where sname='{0}' and tname like '{1}%' and resolution={2}".format(v, tname, res))
        rtables[v] = cur.fetchone()[1]
    cur.close()
    db.close()
    return rtables


def _deleteTableIfExists(dbname, sname, tname):
    """Check if table exists and delete it."""
    db = dbio.connect(dbname)
    cur = db.cursor()
    cur.execute(
        "select * from information_schema.tables where table_schema='{0}' and table_name='{1}'".format(sname, tname))
    if bool(cur.rowcount):
        cur.execute("drop table {0}.{1}".format(sname, tname))
        db.commit()
    cur.close()
    db.close()


def _resampleClimatology(dbname, ptable, name, dt0):
    """Resample finer scale climatology to IRI spatial resolution."""
    tilesize = 10
    res = 2.5
    db = dbio.connect(dbname)
    cur = db.cursor()
    cur.execute(
        "select * from pg_catalog.pg_class c inner join pg_catalog.pg_namespace n on c.relnamespace=n.oid where n.nspname='precip' and c.relname='{0}_iri'".format(ptable))
    if not bool(cur.rowcount):
        sql = "create table precip.{1}_iri as (with f as (select fdate,st_tile(st_rescale(rast,{0},'average'),{2},{2}) as rast from precip.{1}) select fdate,rast,dense_rank() over (order by st_upperleftx(rast),st_upperlefty(rast)) as rid from f)".format(
            res, ptable, tilesize)
        cur.execute(sql)
        cur.execute(
            "create index {0}_iri_r on precip.{0}_iri(rid)".format(ptable))
        cur.execute(
            "create index {0}_iri_t on precip.{0}_iri(fdate)".format(ptable))
        db.commit()
    _deleteTableIfExists(dbname, 'precip', "{0}_iri_xy".format(ptable))
    sql = "create table precip.{0}_iri_xy as (select gid,st_worldtorastercoordx(rast,geom) as x,st_worldtorastercoordy(rast,geom) as y,rid as tile from precip.{0}_iri,{1}.basin where fdate=date'{2}-{3}-{4}' and st_intersects(rast,geom))".format(
        ptable, name, dt0.year, dt0.month, dt0.day)
    cur.execute(sql)
    db.commit()
    cur.execute(
        "create index {0}_iri_xy_r on precip.{0}_iri_xy(tile)".format(ptable))
    db.commit()
    cur.close()
    db.close()


def _getForcings(e, dbname, ptable, rtables, name, dt0, dt1):
    """Extract meteorological forcings for ensemble member."""
    db = dbio.connect(dbname)
    cur = db.cursor()
    data = {}
    for v in ['precip', 'tmax', 'tmin', 'wind']:
        temptable = ''.join(random.SystemRandom().choice(
            string.ascii_letters) for _ in range(8))
        sql = "create table {7} as (with f as (select gid,st_worldtorastercoordx(rast,geom) as xf,st_worldtorastercoordy(rast,geom) as yf,rid as ftile from {6}.{0},{1}.basin where fdate=date'{2}-{3}-{4}' and st_intersects(rast,geom)) select c.gid,xf,yf,x,y,ftile as tile from f inner join precip.{5}_iri_xy as c on c.gid=f.gid)".format(
            rtables[v], name, dt0.year, dt0.month, dt0.day, ptable, v, temptable)
        cur.execute(sql)
        db.commit()
        cur.execute("create index {0}_r on {0}(tile)".format(temptable))
        db.commit()
        sql = "select gid,fdate,st_value(rast,xf,yf) from {6}.{0},{7} as xy inner join iri_years as i on xy.x=i.x and xy.y=i.y where ens={2} and rid=tile and fdate>=date(concat_ws('-',yr,'{3}-{4}')) and fdate<=(date(concat_ws('-',yr,'{3}-{4}'))+interval'{5} days') order by gid,fdate".format(
            rtables[v], ptable, e + 1, dt0.month, dt0.day, (dt1 - dt0).days, v, temptable)
        cur.execute(sql)
        data[v] = cur.fetchall()
        cur.execute("drop table {0}".format(temptable))
        db.commit()
    cur.close()
    db.close()
    return data


def generate(options, models):
    """Generate meteorological forecast forcings by resampling fine-scale climatology."""
    options['vic']['tmax'] = options['vic']['temperature']
    options['vic']['tmin'] = options['vic']['temperature']
    leadtime = 3
    db = dbio.connect(models.dbname)
    cur = db.cursor()
    name = models.name
    dt0 = date(models.startyear, models.startmonth, models.startday)
    dt1 = date(models.endyear, models.endmonth, models.endday)
    dtf = dt0 - relativedelta(months=1)  # forecast initialization date
    months = [(dt0 + relativedelta(months=t)).month for t in range(leadtime)]
    # check if forecast date exists in IRI data
    sql = "select count(*) from precip.iri where fdate=date '{0}-{1}-{2}'".format(
        dtf.year, dtf.month, dtf.day)
    cur.execute(sql)
    if bool(cur.rowcount):
        ptable = options['vic']['precip']
        # find resampled raster tables
        rtables = _getResampledTables(models.dbname, options, models.res)
        # resample climatology to IRI spatial resolution as a table
        _resampleClimatology(models.dbname, ptable, name, dt0)
        # calculate the annual accumulated precipitation using only the months
        # within the forecast period
        _deleteTableIfExists(models.dbname, 'public', 'iri_psum')
        sql = "create table iri_psum as (with f as (select distinct x,y,tile from precip.{0}_iri_xy) select x,y,date_part('year',fdate) as yr,sum(st_value(rast,x,y)) as psum,row_number() over (partition by x,y) as rid from f,precip.{0}_iri where rid=tile and ({1}) group by x,y,yr order by x,y,psum)".format(
            ptable, " or ".join(["date_part('month',fdate)={0}".format(m) for m in months]))
        cur.execute(sql)
        db.commit()
        # retrieve probabilities from IRI seasonal forecast
        _deleteTableIfExists(models.dbname, 'public', 'iri_probs')
        sql = "create table iri_probs as (with f as (select x,y,st_pixelaspoint(rast,x,y) as geom from precip.{0}_iri_xy,precip.{0}_iri where rid=tile and fdate=date'{1}-{2}-{3}') select x,y,st_value(rast,geom) as prob,tercile,leadtime from f,precip.iri where fdate=date'{1}-{2}-{3}')".format(
            ptable, dt0.year, dt0.month, dt0.day)
            # ptable, dt0.year, dt0.month, dt0.day, dtf.year, dtf.month, dtf.day)
        cur.execute(sql)
        cur.execute("alter table iri_probs add column pg int")
        for ti, t in enumerate(['below', 'normal', 'above']):
            cur.execute(
                "update iri_probs set pg={0} where tercile='{1}'".format(ti + 1, t))
        db.commit()
        # get number of years in climatology
        cur.execute("select count(distinct(yr)) from iri_psum")
        nyears = int(cur.fetchone()[0])
        # assign probability weights to each year
        # FIXME: It seems like the IRI NetCDFs have null values for lead times
        # > 1 month. Just using lead time of 1 month for now
        _deleteTableIfExists(models.dbname, 'public', 'iri_pw')
        sql = "create table iri_pw as (with s as (select x,y,yr,psum,rid/({0}/3+1)+1 as pg from iri_psum) select s.x,s.y,s.yr,psum,1.0/{1}*prob/100.0 as weight from s inner join iri_probs as p on p.x=s.x and p.y=s.y and s.pg=p.pg where leadtime=1)".format(
            nyears, nyears / 3.0)
        cur.execute(sql)
        db.commit()
        # sample years based on probability weights
        _deleteTableIfExists(models.dbname, 'public', 'iri_years')
        sql = "create table iri_years as (with f as (select x,y,yr,sum(weight) over (partition by x,y order by psum) as w1, sum(weight) over (partition by x,y order by psum) - weight as w2 from iri_pw), r as (select n as ens,random() as s from generate_series(1,{0}) as x(n)) select x,y,yr,ens from f,r where s>=w2 and s<w1)".format(
            models.nens)
        cur.execute(sql)
        db.commit()
        # retrieve and write forcing data
        for e in range(models.nens):
            data = _getForcings(e, models.dbname, ptable,
                                rtables, name, dt0, dt1)
            models[e].writeForcings(data['precip'], data['tmax'], data[
                                    'tmin'], data['wind'])
    else:
        print(
            "WARNING! IRI forecast was not issued for requested date {0}.".format(dt0))
    # Clean-up temporary tables
    cur.execute("drop table precip.{0}_iri_xy".format(ptable))
    cur.execute("drop table iri_psum")
    cur.execute("drop table iri_probs")
    cur.execute("drop table iri_years")
    cur.close()
    db.close()
