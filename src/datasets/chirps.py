""" RHEAS module for retrieving rainfall data from the Climate Hazard Group
    InfraRed Precipitation with Station (CHIRPS) data archive.

.. module:: chirps
   :synopsis: Retrieve CHIRPS rainfall data

.. moduleauthor:: Kostas Andreadis <kandread@jpl.nasa.gov>

"""

from datasets.decorators import geotiff
import datasets


table = "precip.chirps"


@geotiff
def fetch(dbname, dt, bbox):
    """Downloads CHIRPS rainfall data from the data server."""
    url = "ftp://chg-ftpout.geog.ucsb.edu/pub/org/chg/products/CHIRPS-2.0//africa_daily/tifs/p05/{0:04d}/chirps-v2.0.{0:04d}.{1:02d}.{2:02d}.tif.gz"
    return url, bbox, dt


def download(dbname, dt, bbox=None):
    res = 0.05
    data, lat, lon, t = fetch(dbname, dt, bbox)
    datasets.ingest(dbname, table, data, lat, lon, res, t)


def dates(dbname):
    dts = datasets.dates(dbname, table)
    return dts
