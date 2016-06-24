""" RHEAS module for retrieving the CMORPH rainfall data (daily mean morphed) stored
    at the IRI Data Library.

.. module:: cmorph
   :synopsis: Retrieve CMORPH rainfall data

.. moduleauthor:: Kostas Andreadis <kandread@jpl.nasa.gov>

"""

from datasets.decorators import netcdf
from datetime import timedelta
import datasets


table = "precip.cmorph"


@netcdf
def fetch(dbname, dts, bbox):
    """Downloads CMORPH rainfall data from the IRI data server,
    and imports them into the database *db*. Optionally uses a bounding box to
    limit the region with [minlon, minlat, maxlon, maxlat]."""
    url = "http://iridl.ldeo.columbia.edu/SOURCES/.NOAA/.NCEP/.CPC/.CMORPH/.daily/.mean/.morphed/.cmorph/dods"
    varname = "cmorph"
    return url, varname, bbox, dts


def download(dbname, dts, bbox=None):
    res = 0.25
    data, lat, lon, dts = fetch(dbname, dts, bbox)
    for t, dt in enumerate([dts[0] + timedelta(tt) for tt in range((dts[-1] - dts[0]).days + 1)]):
        datasets.ingest(dbname, table, data[t, :, :], lat, lon, res, dt)


def dates(dbname):
    dts = datasets.dates(dbname, table)
    return dts
