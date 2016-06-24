""" RHEAS module for retrieving maximum, minimum temperature
and wind speed from the MERRA Reanalysis.

.. module:: merra
   :synopsis: Retrieve MERRA meteorological data

.. moduleauthor:: Kostas Andreadis <kandread@jpl.nasa.gov>

"""

import netCDF4 as netcdf
import numpy as np
import os
from datetime import timedelta
import dbio
import datasets


def dates(dbname):
    dts = datasets.dates(dbname, "wind.merra")
    return dts


def _downloadVariable(varname, dbname, dt, bbox):
    """Download specific variable from the MERRA Reanalysis dataset."""
    # FIXME: Grid is not rectangular, but 0.5 x 0.625 degrees
    res = 0.5
    try:
        url = "http://goldsmr4.sci.gsfc.nasa.gov:80/dods/M2T1NXSLV"
        ds = netcdf.Dataset(url)
        lat = ds.variables["lat"][:]
        lon = ds.variables["lon"][:]
        lon[lon > 180] -= 360.0
        i1, i2, j1, j2 = datasets.spatialSubset(np.sort(lat)[::-1], np.sort(lon), res, bbox)
        data = np.zeros((i2-i1, j2-j1))
        lati = np.argsort(lat)[::-1][i1:i2]
        loni = np.argsort(lon)[j1:j2]
        t = ds.variables["time"]
        tt = netcdf.num2date(t[:], units=t.units)
        ti = np.where(tt == dt)[0][0]
        if varname == "tmax":
            hdata = ds.variables["t2m"][ti:ti+24, lati, loni]
            data = np.amax(hdata, axis=0) - 273.15
        elif varname == "tmin":
            hdata = ds.variables["t2m"][ti:ti+24, lati, loni]
            data = np.amin(hdata, axis=0) - 273.15
        elif varname in ["wind"]:
            hdata = np.sqrt(ds.variables["u10m"][ti:ti+24, lati, loni]**2 + ds.variables["v10m"][ti:ti+24, lati, loni]**2)
            data = np.mean(hdata, axis=0)
        lat = np.sort(lat)[::-1][i1:i2]
        lon = np.sort(lon)[j1:j2]
        filename = dbio.writeGeotif(lat, lon, res, data)
        table = "{0}.merra".format(varname)
        dbio.ingest(dbname, filename, dt, table)
        print("Imported {0} in {1}".format(tt[ti].strftime("%Y-%m-%d"), table))
        os.remove(filename)
    except:
        print("Cannot import MERRA dataset for {0}!".format(dt.strftime("%Y-%m-%d")))


def download(dbname, dts, bbox=None):
    """Downloads MERRA Reanalysis data from the NASA data server,
    and imports them into the database *dbname*. Optionally uses a bounding box to
    limit the region with [minlon, minlat, maxlon, maxlat]."""
    for dt in [dts[0] + timedelta(dti) for dti in range((dts[-1] - dts[0]).days + 1)]:
        for varname in ["tmax", "tmin", "wind"]:
            _downloadVariable(varname, dbname, dt, bbox)
