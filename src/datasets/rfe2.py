""" RHEAS module for retrieving the RFEv2 rainfall data stored
    at the IRI Data Library.

.. module:: rfe2
   :synopsis: Retrieve RFEv2 rainfall data

.. moduleauthor:: Kostas Andreadis <kandread@jpl.nasa.gov>

"""

from datasets.decorators import geotiff
import datasets


table = "precip.rfe2"


@geotiff
def fetch(dbname, dt, bbox):
    """Downloads RFE2 rainfall data from the data server."""
    url = "http://ftp.cpc.ncep.noaa.gov/fews/fewsdata/africa/rfe2/geotiff/africa_rfe.{0:04d}{1:02d}{2:02d}.tif.zip"
    return url, bbox, dt


def download(dbname, dt, bbox=None):
    res = 0.10
    data, lat, lon, t = fetch(dbname, dt, bbox)
    datasets.ingest(dbname, table, lat, lon, res, t)


def dates(dbname):
    dts = datasets.dates(dbname, table)
    return dts
