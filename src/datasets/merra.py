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


def _downloadVariable(varname, dbname, dts, bbox):
    """Download specific variable from the MERRA Reanalysis dataset."""
    # FIXME: Grid is not rectangular, but 0.5 x 0.625 degrees
    res = 0.5
    for ts in [dts[0] + timedelta(dti) for dti in range((dts[1] - dts[0]).days + 1)]:
        try:
            url = "http://goldsmr4.sci.gsfc.nasa.gov:80/opendap/MERRA2/M2T1NXSLV.5.12.4/{0}/{1:02d}/MERRA2_400.tavg1_2d_slv_Nx.{0:04d}{1:02d}{2:02d}.nc4".format(ts.year, ts.month, ts.day)
            ds = netcdf.Dataset(url)
            lat = ds.variables["lat"][:]
            lon = ds.variables["lon"][:]
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
            # data = np.zeros((len(i), len(j)))
            data = np.zeros((i2-i1, j2-j1))
            if varname == "tmax":
                # hdata = ds.variables["T2M"][:, i, j]
                hdata = ds.variables["T2M"][:, i1:i2, j1:j2]
                data = np.amax(hdata, axis=0) - 273.15
            elif varname == "tmin":
                # hdata = ds.variables["T2M"][:, i, j]
                hdata = ds.variables["T2M"][:, i1:i2, j1:j2]
                data = np.amin(hdata, axis=0) - 273.15
            elif varname in ["wind"]:
                # hdata = np.sqrt(ds.variables["U10M"][:, i, j]**2 + ds.variables["V10M"][:, i, j]**2)
                hdata = np.sqrt(ds.variables["U10M"][:, i1:i2, j1:j2]**2 + ds.variables["V10M"][:, i1:i2, j1:j2]**2)
                data = np.mean(hdata, axis=0)
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
