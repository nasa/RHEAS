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


def _merraRunid(year):
    """MERRA files use different run IDs depending on the timestamp year."""
    if year < 1992:
        runid = 100
    elif year > 1991 and year < 2001:
        runid = 200
    elif year > 2000 and year < 2011:
        runid = 300
    else:
        runid = 400
    return runid


def _downloadVariable(varname, dbname, dts, bbox):
    """Download specific variable from the MERRA Reanalysis dataset."""
    # FIXME: Grid is not rectangular, but 0.5 x 0.625 degrees
    res = 0.5
    for ts in [dts[0] + timedelta(dti) for dti in range((dts[1] - dts[0]).days + 1)]:
        try:
            runid = _merraRunid(ts.year)
            url = "http://goldsmr4.sci.gsfc.nasa.gov:80/opendap/MERRA2/M2T1NXSLV.5.12.4/{1}/{2:02d}/MERRA2_{0}.tavg1_2d_slv_Nx.{1:04d}{2:02d}{3:02d}.nc4".format(runid, ts.year, ts.month, ts.day)
            ds = netcdf.Dataset(url)
            lat = ds.variables["lat"][:]
            lon = ds.variables["lon"][:]
            lon[lon > 180] -= 360.0
            i1, i2, j1, j2 = datasets.spatialSubset(np.sort(lat)[::-1], np.sort(lon), res, bbox)
            data = np.zeros((i2-i1, j2-j1))
            lati = np.argsort(lat)[::-1][i1:i2]
            loni = np.argsort(lon)[j1:j2]
            if varname == "tmax":
                hdata = ds.variables["T2M"][:, lati, loni]
                data = np.amax(hdata, axis=0) - 273.15
            elif varname == "tmin":
                hdata = ds.variables["T2M"][:, lati, loni]
                data = np.amin(hdata, axis=0) - 273.15
            elif varname in ["wind"]:
                hdata = np.sqrt(ds.variables["U10M"][:, lati, loni]**2 + ds.variables["V10M"][:, lati, loni]**2)
                data = np.mean(hdata, axis=0)
            lat = np.sort(lat)[::-1][i1:i2]
            lon = np.sort(lon)[j1:j2]
            filename = dbio.writeGeotif(lat, lon, res, data)
            dbio.ingest(dbname, filename, ts, "{0}.merra".format(varname))
            os.remove(filename)
        except:
            print("Cannot import MERRA dataset for {0}!".format(ts.strftime("%Y-%m-%d")))


def download(dbname, dt, bbox=None):
    """Downloads MERRA Reanalysis data from the NASA data server,
    and imports them into the database *dbname*. Optionally uses a bounding box to
    limit the region with [minlon, minlat, maxlon, maxlat]."""
    for varname in ["tmax", "tmin", "wind"]:
        _downloadVariable(varname, dbname, dt, bbox)
