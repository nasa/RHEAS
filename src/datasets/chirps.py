""" RHEAS module for retrieving rainfall data from the Climate Hazard Group
    InfraRed Precipitation with Station (CHIRPS) data archive.

.. module:: chirps
   :synopsis: Retrieve CHIRPS rainfall data

.. moduleauthor:: Kostas Andreadis <kandread@jpl.nasa.gov>

"""

from datetime import datetime, timedelta
from datasets.decorators import geotiff, http
import datasets


table = "precip.chirps"


@geotiff
@http
def fetch(dbname, dt, bbox):
    """Downloads CHIRPS rainfall data from the data server."""
    url = "ftp://ftp.chg.ucsb.edu/pub/org/chg/products/CHIRPS-2.0/global_daily/tifs/p05/{0:04d}/chirps-v2.0.{0:04d}.{1:02d}.{2:02d}.tif.gz"
    return url, bbox, dt


def download(dbname, dts, bbox=None):
    res = 0.05
    for dt in [dts[0] + timedelta(tt) for tt in range((dts[-1] - dts[0]).days + 1)]:
        data, lat, lon, t = fetch(dbname, dt, bbox)
        datasets.ingest(dbname, table, data, lat, lon, res, t)


def dates(dbname):
    dts = datasets.dates(dbname, table)
    return dts
