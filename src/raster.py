""" RHEAS module for raster manipulation within the PostGIS database

.. module:: raster
   :synopsis: Manipulate PostGIS rasters

.. moduleauthor:: Kostas Andreadis <kandread@jpl.nasa.gov>

"""

import dbio
import logging


class TileReader:
    """Helper class to retrieve raster tile from database."""

    def __init__(self, dbname, rtable, startyear, startmonth, startday, endyear, endmonth, endday):
        self.dbname = dbname
        self.rtable = rtable
        self.startyear = startyear
        self.startmonth = startmonth
        self.startday = startday
        self.endyear = endyear
        self.endmonth = endmonth
        self.endday = endday

    def __call__(self, t):
        db = dbio.connect(self.dbname)
        cur = db.cursor()
        var = self.rtable.split(".")[0]
        sql = "select gid,fdate,st_nearestvalue(rast,x,y) from {0},{1}_xy where rid=tile and tile={8} and fdate>=date'{2}-{3}-{4}' and fdate<=date'{5}-{6}-{7}' order by gid,fdate".format(
            self.rtable, var, self.startyear, self.startmonth, self.startday, self.endyear, self.endmonth, self.endday, t)
        cur.execute(sql)
        data = cur.fetchall()
        return data


def _columnExists(cursor, name, colname):
    """Tests whether a column exists in a table."""
    schemaname, tablename = name.split(".")
    sql = "select column_name from information_schema.columns where table_schema='{0}' and table_name='{1}' and column_name='{2}'".format(
        schemaname, tablename, colname)
    cursor.execute(sql)
    return bool(cursor.rowcount)


def stddev(dbname, name):
    """Calculate ensemble standard deviation from raster."""
    log = logging.getLogger(__name__)
    schemaname, tablename = name.split(".")
    db = dbio.connect(dbname)
    cur = db.cursor()
    if _columnExists(cur, name, "ensemble"):
        cur.execute("select max(ensemble) from {0}".format(name))
        nens = cur.fetchone()[0]
        ssql = "select fdate,st_mapalgebra(st_addband(null,array_agg(rast)),ARRAY{0},'st_stddev4ma(float[][],text,text[])'::regprocedure) as rast from {1} group by fdate".format(
            str(range(1, nens + 1)), name)
        sql = "select * from information_schema.columns where table_schema='{0}' and table_name='{1}_std'".format(
            schemaname, tablename)
        cur.execute(sql)
        if bool(cur.rowcount):
            cur.execute("drop table {0}_std".format(name))
        sql = "create table {0}.{1}_std as ({2})".format(
            schemaname, tablename, ssql)
        cur.execute(sql)
    else:
        log.warning("Cannot calculate uncertainty maps, no ensemble exists.")
    db.commit()
    cur.close()
    db.close()


def mean(dbname, name):
    """Calculate ensemble average from raster."""
    log = logging.getLogger(__name__)
    schemaname, tablename = name.split(".")
    db = dbio.connect(dbname)
    cur = db.cursor()
    if _columnExists(cur, name, "ensemble"):
        cur.execute("select max(ensemble) from {0}".format(name))
        nens = cur.fetchone()[0]
        ssql = "select fdate,st_union(rast,'MEAN') as rast from {0} group by fdate".format(name)
        sql = "select * from information_schema.columns where table_schema='{0}' and table_name='{1}_mean'".format(
            schemaname, tablename)
        cur.execute(sql)
        if bool(cur.rowcount):
            cur.execute("drop table {0}_mean".format(name))
        sql = "create table {0}.{1}_mean as ({2})".format(schemaname, tablename, ssql)
        cur.execute(sql)
    else:
        log.warning("Cannot calculate ensemble average maps, no ensemble exists.")
    db.commit()
    cur.close()
    db.close()
