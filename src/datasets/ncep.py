""" RHEAS module for retrieving maximum and minimum
temperature from the NCEP Reanalysis stored at the IRI Data Library.

.. module:: ncep
   :synopsis: Retrieve NCEP meteorological data

.. moduleauthor:: Kostas Andreadis <kandread@jpl.nasa.gov>

"""

import netCDF4 as netcdf
import numpy as np
import os
from datetime import timedelta
import dbio
import datasets
from decorators import resetDatetime


def dates(dbname):
    dts = datasets.dates(dbname, "wind.ncep")
    return dts


def _downloadVariable(varname, dbname, dt, bbox=None):
    """Download specific variable from the NCEP Reanalysis dataset."""
    res = 1.875
    if varname == "tmax":
        urls = ["http://iridl.ldeo.columbia.edu/SOURCES/.NOAA/.NCEP-NCAR/.CDAS-1/.DAILY/.Diagnostic/.above_ground/.maximum/.temp/dods"]
        dsvar = ["temp"]
    elif varname == "tmin":
        urls = ["http://iridl.ldeo.columbia.edu/SOURCES/.NOAA/.NCEP-NCAR/.CDAS-1/.DAILY/.Diagnostic/.above_ground/.minimum/.temp/dods"]
        dsvar = ["temp"]
    else:
        urls = ["http://iridl.ldeo.columbia.edu/SOURCES/.NOAA/.NCEP-NCAR/.CDAS-1/.DAILY/.Diagnostic/.above_ground/.u/dods",
                "http://iridl.ldeo.columbia.edu/SOURCES/.NOAA/.NCEP-NCAR/.CDAS-1/.DAILY/.Diagnostic/.above_ground/.v/dods"]
        dsvar = ["u", "v"]
    data = None
    for ui, url in enumerate(urls):
        pds = netcdf.Dataset(url)
        lat = pds.variables["Y"][:]
        lon = pds.variables["X"][:]
        lon[lon > 180] -= 360.0
        i1, i2, j1, j2 = datasets.spatialSubset(np.sort(lat)[::-1], np.sort(lon), res, bbox)
        t = pds.variables["T"]
        tt = netcdf.num2date(t[:], units=t.units)
        # ti = [tj for tj in range(len(tt)) if tt[tj] >= dt]
        ti = [tj for tj in range(len(tt)) if resetDatetime(tt[tj]) >= dt[0] and resetDatetime(tt[tj]) <= dt[1]]
        if len(ti) > 0:
            lati = np.argsort(lat)[::-1][i1:i2]
            loni = np.argsort(lon)[j1:j2]
            if data is None:
                data = pds.variables[dsvar[ui]][ti, 0, lati, loni]
            else:
                data = np.sqrt(
                    data ** 2.0 + pds.variables[dsvar[ui]][ti, 0, lati, loni] ** 2.0)
        if "temp" in dsvar:
            data -= 273.15
        lat = np.sort(lat)[::-1][i1:i2]
        lon = np.sort(lon)[j1:j2]
    table = "{0}.ncep".format(varname)
    for t in range(len(ti)):
        filename = dbio.writeGeotif(lat, lon, res, data[t, :, :])
        dbio.ingest(dbname, filename, tt[ti[t]], table)
        print("Imported {0} in {1}".format(tt[ti[0]].strftime("%Y-%m-%d"), table))
        os.remove(filename)


def download(dbname, dts, bbox=None):
    """Downloads NCEP Reanalysis data from the IRI data server,
    and imports them into the database *db*. Optionally uses a bounding box to
    limit the region with [minlon, minlat, maxlon, maxlat]."""
    # for dt in [dts[0] + timedelta(tt) for tt in range((dts[1] - dts[0]).days + 1)]:
    for varname in ["tmax", "tmin", "wind"]:
        _downloadVariable(varname, dbname, dts, bbox)
