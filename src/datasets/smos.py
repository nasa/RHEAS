"""Class definition for the SMOS Soil Mositure data type.

.. module:: smos
   :synopsis: Definition of the SMOS class

.. moduleauthor:: Kostas Andreadis <kandread@jpl.nasa.gov>

"""

from soilmoist import Soilmoist
import dbio
import os
import netCDF4 as netcdf
from datetime import datetime, timedelta
import datasets
import rpath
import logging


table = "soilmoist.smos"


def dates(dbname):
    dts = datasets.dates(dbname, table)
    return dts


def download(dbname, dt, bbox=None):
    """Downloads SMOS soil mositure data for a set of dates *dt*
    and imports them into the PostGIS database *dbname*. Optionally
    uses a bounding box to limit the region with [minlon, minlat, maxlon, maxlat]."""
    log = logging.getLogger(__name__)
    res = 0.25
    url = "http://rheas:rheasjpl@cp34-bec.cmima.csic.es/thredds/dodsC/NRTSM001D025A_ALL"
    f = netcdf.Dataset(url)
    lat = f.variables['lat'][::-1]  # swap latitude orientation to northwards
    lon = f.variables['lon'][:]
    i1, i2, j1, j2 = datasets.spatialSubset(lat, lon, res, bbox)
    smi1 = len(lat) - i2 - 1
    smi2 = len(lat) - i1 - 1
    lat = lat[i1:i2]
    lon = lon[j1:j2]
    t0 = datetime(2010, 1, 12)  # initial date of SMOS data
    t1 = (dt[0] - t0).days
    if t1 < 0:
        log.warning("Reseting start date to {0}".format(t0.strftime("%Y-%m-%d")))
        t1 = 0
    t2 = (dt[-1] - t0).days + 1
    nt, _, _ = f.variables['SM'].shape
    if t2 > nt:
        t2 = nt
        log.warning("Reseting end date to {0}".format((t0 + timedelta(t2)).strftime("%Y-%m-%d")))
    ti = range(t1, t2)
    sm = f.variables['SM'][ti, smi1:smi2, j1:j2]
    # FIXME: Use spatially variable observation error
    # smv = f.variables['VARIANCE_SM'][ti, i1:i2, j1:j2]
    for tj in range(sm.shape[0]):
        # filename = dbio.writeGeotif(lat, lon, res, sm[tj, :, :])
        t = t0 + timedelta(ti[tj])
        if not os.path.isdir("{0}/soilmoist/smos".format(rpath.data)):
            os.mkdir("{0}/soilmoist/smos".format(rpath.data))
        filename = "{0}/soilmoist/smos/smos_{1}.tif".format(rpath.data, t.strftime("%Y%m%d"))
        dbio.writeGeotif(lat, lon, res, sm[tj, :, :], filename)
        dbio.ingest(dbname, filename, t, table, False)
        log.info("Imported SMOS {0}".format(tj))
        os.remove(filename)


class Smos(Soilmoist):

    def __init__(self, uncert=None):
        """Initialize SMOS soil moisture object."""
        super(Smos, self).__init__(uncert)
        self.res = 0.25
        self.stddev = 0.01
        self.tablename = "soilmoist.smos"
