"""Class definition for the SMOS Soil Mositure data type.

.. module:: smos
   :synopsis: Definition of the SMOS class

.. moduleauthor:: Kostas Andreadis <kandread@jpl.nasa.gov>

"""

import dbio
import numpy as np
import os
import netCDF4 as netcdf
from datetime import datetime, timedelta
import datasets


table = "soilmoist.smos"


def dates(dbname):
    dts = datasets.dates(dbname, table)
    return dts


def download(dbname, dt, bbox=None):
    """Downloads SMOS soil mositure data for a set of dates *dt*
    and imports them into the PostGIS database *dbname*. Optionally
    uses a bounding box to limit the region with [minlon, minlat, maxlon, maxlat]."""
    res = 0.25
    url = "http://rheas:rheasjpl@cp34-bec.cmima.csic.es/thredds/dodsC/NRTSM001D025A_ALL"
    f = netcdf.Dataset(url)
    lat = f.variables['lat'][:]
    lon = f.variables['lon'][:]
    if bbox is not None:
        i = np.where(np.logical_and(lat > bbox[1], lat < bbox[3]))[0]
        j = np.where(np.logical_and(lon > bbox[0], lon < bbox[2]))[0]
        lat = lat[i]
        lon = lon[j]
    else:
        i = range(len(lat))
        j = range(len(lon))
    t0 = datetime(2010, 1, 12)  # initial date of SMOS data
    t1 = (dt[0] - t0).days
    t2 = (dt[1] - t0).days + 1
    ti = range(t1, t2)
    sm = f.variables['SM'][ti, i, j]
    # FIXME: Use spatially variable observation error
    # smv = f.variables['VARIANCE_SM'][ti, i, j]
    for tj in range(sm.shape[0]):
        filename = dbio.writeGeotif(lat, lon, res, sm[tj, :, :])
        t = t0 + timedelta(ti[tj])
        dbio.ingest(dbname, filename, t, table, False)
        print("Imported SMOS {0}".format(tj))
        os.remove(filename)


class Smos(object):

    def __init__(self):
        """Initialize SMOS soil moisture object."""
        super(Smos, self).__init__()
        self.res = 0.25
        self.stddev = 0.01
        self.tablename = "soilmoist.smos"
