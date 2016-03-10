""" RHEAS module for retrieving the CMORPH rainfall data (daily mean morphed) stored
    at the IRI Data Library.

.. module:: cmorph
   :synopsis: Retrieve CMORPH rainfall data

.. moduleauthor:: Kostas Andreadis <kandread@jpl.nasa.gov>

"""

from datasets.decorators import netcdf
import datasets


table = "precip.cmorph"


@netcdf
def fetch(dbname, dt, bbox):
    """Downloads CMORPH rainfall data from the IRI data server,
    and imports them into the database *db*. Optionally uses a bounding box to
    limit the region with [minlon, minlat, maxlon, maxlat]."""
    url = "http://iridl.ldeo.columbia.edu/SOURCES/.NOAA/.NCEP/.CPC/.CMORPH/.daily/.mean/.morphed/.cmorph/dods"
    varname = "cmorph"
    return url, varname, bbox, dt


def download(dbname, dt, bbox=None):
    res = 0.25
    data, lat, lon, t = fetch(dbname, dt, bbox)
    datasets.ingest(dbname, table, data, lat, lon, res, t)


def dates(dbname):
    dts = datasets.dates(dbname, table)
    return dts
