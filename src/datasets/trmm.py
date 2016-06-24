""" RHEAS module for retrieving the TRMM rainfall data (3B42v7) stored
    at the IRI Data Library.

.. module:: trmm
   :synopsis: Retrieve TRMM rainfall data

.. moduleauthor:: Kostas Andreadis <kandread@jpl.nasa.gov>

"""

from datasets.decorators import netcdf
from datetime import timedelta
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


def download(dbname, dts, bbox=None):
    res = 0.25
    data, lat, lon, dts = fetch(dbname, dts, bbox)
    for t, dt in enumerate([dts[0] + timedelta(tt) for tt in range((dts[-1] - dts[0]).days + 1)]):
        datasets.ingest(dbname, table, data[t, :, :], lat, lon, res, dt)


def dates(dbname):
    dts = datasets.dates(dbname, table)
    return dts
