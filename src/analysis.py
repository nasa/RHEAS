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
        except:
            log.warning("Start date is invalid and will be ignored.")
    if len(enddate) > 0:
        try:
            edt = datetime.strptime(enddate, "%Y-%m-%d")
            datesql += "and fdate<=date'{0}'".format(edt.strftime("%y-%m-%d"))
        except:
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
