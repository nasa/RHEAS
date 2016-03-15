""" RHEAS module for retrieving the RFEv2 rainfall data stored
    at the IRI Data Library.

.. module:: rfe2
   :synopsis: Retrieve RFEv2 rainfall data

.. moduleauthor:: Kostas Andreadis <kandread@jpl.nasa.gov>

"""

from datasets.decorators import geotiff, http
from datetime import timedelta
import datasets


table = "precip.rfe2"


@geotiff
@http
def fetch(dbname, dt, bbox):
    """Downloads RFE2 rainfall data from the data server."""
    url = "http://ftp.cpc.ncep.noaa.gov/fews/fewsdata/africa/rfe2/geotiff/africa_rfe.{0:04d}{1:02d}{2:02d}.tif.zip"
    return url, bbox, dt


def download(dbname, dts, bbox=None):
    res = 0.10
    for dt in [dts[0] + timedelta(tt) for tt in range((dts[1] - dts[0]).days + 1)]:
        data, lat, lon, t = fetch(dbname, dt, bbox)
        datasets.ingest(dbname, table, data, lat, lon, res, t)


def dates(dbname):
    dts = datasets.dates(dbname, table)
    return dts
