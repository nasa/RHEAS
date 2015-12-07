""" RHEAS module for retrieving the TRMM rainfall data (3B42v7) stored
    at the IRI Data Library.

.. module:: trmm
   :synopsis: Retrieve TRMM rainfall data

.. moduleauthor:: Kostas Andreadis <kandread@jpl.nasa.gov>

"""

from datasets.decorators import netcdf
import datasets


table = "precip.trmm"


@netcdf
def fetch(dbname, dt, bbox):
    """Downloads TRMM 3B42v7 rainfall data from the IRI data server,
    and imports them into the database *dbname*. Optionally uses a bounding box to
    limit the region with [minlon, minlat, maxlon, maxlat]."""
    url = "http://iridl.ldeo.columbia.edu/SOURCES/.NASA/.GES-DAAC/.TRMM_L3/.TRMM_3B42/.v7/.daily/.precipitation/dods"
    varname = "precipitation"
    return url, varname, bbox, dt


def download(dbname, dt, bbox=None):
    res = 0.25
    data, lat, lon, t = fetch(dbname, dt, bbox)
    datasets.ingest(dbname, table, data, lat, lon, res, t)


def dates(dbname):
    dts = datasets.dates(dbname, table)
    return dts
