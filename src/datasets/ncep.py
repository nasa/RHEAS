""" RHEAS module for retrieving maximum and minimum
temperature from the NCEP Reanalysis stored at the IRI Data Library.

.. module:: ncep
   :synopsis: Retrieve NCEP meteorological data

.. moduleauthor:: Kostas Andreadis <kandread@jpl.nasa.gov>

"""

import netCDF4 as netcdf
import numpy as np
import os
import dbio
import datasets


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
        i1, i2, j1, j2 = datasets.spatialSubset(lat, lon, res, bbox)
        lat = lat[i1:i2]
        lon = lon[j1:j2]
        # if bbox is not None:
        #     i = np.where(np.logical_and(lat > bbox[1], lat < bbox[3]))[0]
        #     j = np.where(np.logical_and(lon > bbox[0], lon < bbox[2]))[0]
        #     lat = lat[i]
        #     lon = lon[j]
        # else:
        #     i = range(len(lat))
        #     j = range(len(lon))
        t = pds.variables["T"]
        tt = netcdf.num2date(t[:], units=t.units)
        ti = [tj for tj in range(len(tt)) if tt[tj] >= dt[
            0] and tt[tj] <= dt[1]]
        if data is None:
            # data = pds.variables[dsvar[ui]][ti, 0, i, j]
            data = pds.variables[dsvar[ui]][ti, 0, i1:i2, j1:j2]
        else:
            # data = np.sqrt(
            #     data ** 2.0 + pds.variables[dsvar[ui]][ti, 0, i, j] ** 2.0)
            data = np.sqrt(
                data ** 2.0 + pds.variables[dsvar[ui]][ti, 0, i1:i2, j1:j2] ** 2.0)
    if "temp" in dsvar:
        data -= 273.15
    for tj in range(data.shape[0]):
        filename = dbio.writeGeotif(lat, lon, res, data[tj, :, :])
        dbio.ingest(dbname, filename, tt[ti[tj]], "{0}.ncep".format(varname))
        os.remove(filename)


def download(dbname, dt, bbox=None):
    """Downloads NCEP Reanalysis data from the IRI data server,
    and imports them into the database *db*. Optionally uses a bounding box to
    limit the region with [minlon, minlat, maxlon, maxlat]."""
    for varname in ["tmax", "tmin", "wind"]:
        _downloadVariable(varname, dbname, dt, bbox)
