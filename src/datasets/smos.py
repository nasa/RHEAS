"""Class definition for the SMOS Soil Mositure data type.

.. module:: smos
   :synopsis: Definition of the SMOS class

.. moduleauthor:: Kostas Andreadis <kandread@jpl.nasa.gov>

"""

from soilmoist import Soilmoist
import dbio
import os
import netCDF4 as netcdf
from scipy.spatial import KDTree
import numpy as np
from datetime import datetime, timedelta
import datasets
import logging


table = "soilmoist.smos"


def dates(dbname):
    dts = datasets.dates(dbname, table)
    return dts


def regridNearestNeighbor(lat, lon, res):
    """Generate grid of nearest neighbor locations from *lat*, *lon*
    arrays for specified resolution *res*."""
    x, y = np.meshgrid(lon, lat)
    tree = KDTree(zip(x.ravel(), y.ravel()))
    grid_lat = np.arange(round(lat[0], 1), round(lat[-1], 1)-res, -res)
    grid_lon = np.arange(round(lon[0], 1), round(lon[-1], 1)+res, res)
    grid_x, grid_y = np.meshgrid(grid_lon, grid_lat)
    _, pos = tree.query(zip(grid_x.ravel(), grid_y.ravel()))
    return pos, grid_lat, grid_lon


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
    sm = sm[:, ::-1, :]  # flip latitude dimension in data array
    # FIXME: Use spatially variable observation error
    # smv = f.variables['VARIANCE_SM'][ti, i1:i2, j1:j2][:, ::-1, :]
    pos, smlat, smlon = regridNearestNeighbor(lat, lon, res)
    for tj in range(sm.shape[0]):
        smdata = sm[tj, :, :].ravel()[pos].reshape((len(smlat), len(smlon)))
        filename = dbio.writeGeotif(smlat, smlon, res, smdata)
        t = t0 + timedelta(ti[tj])
        dbio.ingest(dbname, filename, t, table, False)
        os.remove(filename)


class Smos(Soilmoist):

    def __init__(self, uncert=None):
        """Initialize SMOS soil moisture object."""
        super(Smos, self).__init__(uncert)
        self.res = 0.25
        self.stddev = 0.01
        self.tablename = "soilmoist.smos"
