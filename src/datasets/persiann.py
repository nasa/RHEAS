""" RHEAS module for retrieving the PERSIANN rainfall data stored
    at the IRI Data Library.

.. module:: persiann
   :synopsis: Retrieve PERSIANN rainfall data

.. moduleauthor:: Kostas Andreadis <kandread@jpl.nasa.gov>

"""

from datetime import timedelta
from datasets.decorators import opendap
import datasets


table = "precip.persiann"


@opendap
def fetch(dbname, dts, bbox):
    """Downloads CMORPH rainfall data from the IRI data server,
    and imports them into the database *db*. Optionally uses a bounding box to
    limit the region with [minlon, minlat, maxlon, maxlat]."""
    url = "http://iridl.ldeo.columbia.edu/SOURCES/.NOAA/.NCDC/.CDR/.PERSIANN/.v01r01/.precipitation/dods"
    varname = "precipitation"
    return url, varname, bbox, dts


def download(dbname, dts, bbox=None):
    res = 0.25
    data, lat, lon, dts = fetch(dbname, dts, bbox)
    data *= 24.0  # convert from mm/hr to mm
    for t, dt in enumerate([dts[0] + timedelta(tt) for tt in range((dts[-1] - dts[0]).days + 1)]):
        datasets.ingest(dbname, table, data[t, :, :], lat, lon, res, dt)


def dates(dbname):
    dts = datasets.dates(dbname, table)
    return dts

