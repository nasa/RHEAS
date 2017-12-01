""" RHEAS analysis module.

.. module:: analysis
   :synopsis: Module for post-processing RHEAS outputs

.. moduleauthor:: Kostas Andreadis <kandread@jpl.nasa.gov>

"""

import dbio
import logging
from osgeo import ogr
from datetime import datetime


def cropYield(shapefile, name, startdate="", enddate="", crop="maize", dbname="rheas"):
    """Extract crop yield from a specified simulation *name* for dates ranging
    from *startdate* to *enddate*, and saves them a *shapefile*."""
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    log = logging.getLogger(__name__)
    db = dbio.connect(dbname)
    cur = db.cursor()
    datesql = ""
    if len(startdate) > 0:
        try:
            sdt = datetime.strptime(startdate, "%Y-%m-%d")
            datesql = "and fdate>=date'{0}'".format(sdt.strftime("%Y-%m-%d"))
        except ValueError:
            log.warning("Start date is invalid and will be ignored.")
    if len(enddate) > 0:
        try:
            edt = datetime.strptime(enddate, "%Y-%m-%d")
            datesql += "and fdate<=date'{0}'".format(edt.strftime("%y-%m-%d"))
        except ValueError:
            log.warning("End date is invalid and will be ignored.")
    fsql = "with f as (select gid,geom,gwad,ensemble,fdate from (select gid,geom,gwad,ensemble,fdate,row_number() over (partition by gid,ensemble order by gwad desc) as rn from {0}.dssat) gwadtable where rn=1 {1})".format(name, datesql)
    sql = "{0} select gid,st_astext(geom),max(gwad) as max_yield,avg(gwad) as avg_yield,stddev(gwad) as std_yield,max(fdate) as fdate from f group by gid,geom".format(fsql)
    cur.execute(sql)
    if bool(cur.rowcount):
        results = cur.fetchall()
        drv = ogr.GetDriverByName("ESRI Shapefile")
        ds = drv.CreateDataSource(shapefile)
        lyr = ds.CreateLayer("yield", geom_type=ogr.wkbMultiPolygon)
        lyr.CreateField(ogr.FieldDefn("gid", ogr.OFTInteger))
        lyr.CreateField(ogr.FieldDefn("average", ogr.OFTReal))
        lyr.CreateField(ogr.FieldDefn("maximum", ogr.OFTReal))
        lyr.CreateField(ogr.FieldDefn("minimum", ogr.OFTReal))
        for row in results:
            feat = ogr.Feature(lyr.GetLayerDefn())
            feat.SetField("gid", row[0])
            feat.SetField("maximum", row[2])
            feat.SetField("average", row[3])
            feat.SetField("minimum", row[4])
            feat.SetGeometry(ogr.CreateGeometryFromWkt(row[1]))
            lyr.CreateFeature(feat)
            feat.Destroy()
        ds.Destroy()


def saveVariable(filepath, name, varname, startdate="", enddate="", dbname="rheas"):
    """Extracts geophysical variable *varname* from simulation *name* for date range between
    *startdate* and *enddate*, and either saves the rasters if *filepath* is a
    directory, or as a CSV file with each column associated to a polygon of the
    *filepath* shapefile."""
    if filepath.endswith(".shp"):
        pass
    else:
        _saveRasters(filepath, name, varname, startdate, enddate, dbname)


def _saveRasters(filepath, name, varname, startdate, enddate, dbname):
    """"Save geophysical variable from *dbname* database, between *startdate*
    and *enddate* dates into Geotif files inside *filepath* directory."""
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    log = logging.getLogger(__name__)
    if dbio.tableExists(dbname, name, varname):
        db = dbio.connect(dbname)
        cur = db.cursor()
        sql = "select fdate,st_astiff(st_union(rast)) as tif from {0}.{1}".format(name, varname)
        try:
            sdt = datetime.strptime(startdate, "%Y-%m-%d")
            edt = datetime.strptime(enddate, "%Y-%m-%d")
            sql += " where fdate>=date'{0}' and fdate<=date'{1} group by fdate".format(sdt.strftime("%Y-%m-%d"), edt.strftime("%Y-%m-%d"))
        except ValueError:
            sql += " group by fdate"
            log.warning("Start and/or end dates were invalid. Ignoring...")
        cur.execute(sql)
        results = cur.fetchall()
        for res in results:
            with open("{0}/{1}_{2}.tif".format(filepath, varname, res[0].strftime("%Y%m%d")), 'wb') as fout:
                fout.write(res[1])
    else:
        log.error("Variable {0} does not exist in schema {1}.".format(varname, name))
