""" Definition for RHEAS Datasets package.

.. module:: datasets
   :synopsis: Definition of the Datasets package

.. moduleauthor:: Kostas Andreadis <kandread@jpl.nasa.gov>

"""

import os
import ConfigParser
import sys
import dbio
from datetime import datetime, timedelta
import numpy as np


def readDatasetList(filename):
    """Read list of datasets to be fetched and imported into
    the RHEAS database."""
    conf = ConfigParser.ConfigParser()
    try:
        conf.read(filename)
    except:
        print "ERROR! File not found: {}".format(filename)
        sys.exit()
    return conf


def dates(dbname, tablename):
    """Check what dates need to be imported for a specific dataset."""
    dts = None
    db = dbio.connect(dbname)
    cur = db.cursor()
    sname, tname = tablename.split(".")
    cur.execute(
        "select * from information_schema.tables where table_name='{0}' and table_schema='{1}'".format(tname, sname))
    if bool(cur.rowcount):
        sql = "select max(fdate) from {0}".format(tablename)
        cur.execute(sql)
        te = cur.fetchone()[0]
        te = datetime(te.year, te.month, te.day)
        if te < datetime.today():
            dts = (te + timedelta(1), datetime.today())
    else:
        dts = None
    return dts


def spatialSubset(lat, lon, res, bbox):
    """Subsets arrays of latitude/longitude based on bounding box *bbox*."""
    if bbox is None:
        i1 = 0
        i2 = len(lat)-1
        j1 = 0
        j2 = len(lat)-1
    else:
        i1 = np.where(np.logical_and(bbox[1] >= lat-res/2, bbox[1] <= lat+res/2))[0][0]
        i2 = np.where(np.logical_and(bbox[3] >= lat-res/2, bbox[3] <= lat+res/2))[0][-1]
        j1 = np.where(np.logical_and(bbox[0] >= lon-res/2, bbox[0] <= lon+res/2))[0][0]
        j2 = np.where(np.logical_and(bbox[2] >= lon-res/2, bbox[2] <= lon+res/2))[0][-1]
        # account for latitude arrays oriented northwards
        if i1 > i2:
            i1, i2 = i2, i1
    return i1, i2+1, j1, j2+1


def download(dbname, conf):
    """Download a generic dataset based on user-provided information."""
    pass


def ingest(dbname, table, data, lat, lon, res, t):
    """Import data into RHEAS database."""
    if data is not None:
        for tj in range(data.shape[0]):
            filename = dbio.writeGeotif(lat, lon, res, data[tj, :, :])
            dbio.ingest(dbname, filename, t[tj], table)
            print("Imported {0} in {1}".format(t[tj].strftime("%Y-%m-%d"), table))
            os.remove(filename)
    else:
        print("WARNING! No data were available to import into {0}.".format(table))
