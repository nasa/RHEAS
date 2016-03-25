""" RHEAS module for retrieving rainfall data from the Climate Hazard Group
    InfraRed Precipitation with Station (CHIRPS) data archive.

.. module:: chirps
   :synopsis: Retrieve CHIRPS rainfall data

.. moduleauthor:: Kostas Andreadis <kandread@jpl.nasa.gov>

"""

from datetime import datetime, timedelta
import netCDF4 as netcdf
import datasets


table = "precip.chirps"


def download(dbname, dts, bbox):
    """Downloads CHIRPS rainfall data from the IRI data server,
    and imports them into the database *db*. Optionally uses a bounding box to
    limit the region with [minlon, minlat, maxlon, maxlat]."""
    res = 0.05
    url = "http://iridl.ldeo.columbia.edu/SOURCES/.UCSB/.CHIRPS/.v2p0/.daily/.global/.0p05/.prcp/dods"
    f = netcdf.Dataset(url)
    lat = f.variables['Y'][:]
    lon = f.variables['X'][:]
    i1, i2, j1, j2 = datasets.spatialSubset(lat, lon, res, bbox)
    lat = lat[i1:i2]
    lon = lon[j1:j2]
    ti = range((dts[0] - datetime(1981, 1, 1)).days, (dts[1] - datetime(1981, 1, 1)).days + 1)
    data = f.variables['prcp'][ti, i1:i2, j1:j2]
    for tj in range(len(ti)):
        dt = dts[0] + timedelta(tj)
        datasets.ingest(dbname, table, data, lat, lon, res, dt)


def dates(dbname):
    dts = datasets.dates(dbname, table)
    return dts
