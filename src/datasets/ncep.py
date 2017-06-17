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
import rpath
from decorators import resetDatetime
import logging
from datetime import timedelta, datetime


def dates(dbname):
    dts = datasets.dates(dbname, "wind.ncep")
    return dts


def _downloadVariable(varname, dbname, dt, bbox=None):
    """Download specific variable from the NCEP Reanalysis dataset."""
    log = logging.getLogger(__name__)
    res = 1.875
    baseurl = "http://www.esrl.noaa.gov/psd/thredds/dodsC/Datasets/ncep.reanalysis.dailyavgs/surface_gauss"
    if varname == "tmax":
        urls = ["{0}/tmax.2m.gauss.{1}.nc".format(baseurl, dt[0].year)]
        dsvar = ["tmax"]
    elif varname == "tmin":
        urls = ["{0}/tmin.2m.gauss.{1}.nc".format(baseurl, dt[0].year)]
        dsvar = ["tmin"]
    else:
        urls = ["{0}/uwnd.10m.gauss.{1}.nc".format(baseurl, dt[0].year), "{0}/vwnd.10m.gauss.{1}.nc".format(baseurl, dt[0].year)]
        dsvar = ["uwnd", "vwnd"]
    data = None
    for ui, url in enumerate(urls):
        pds = netcdf.Dataset(url)
        lat = pds.variables["lat"][:]
        lon = pds.variables["lon"][:]
        lon[lon > 180] -= 360.0
        i1, i2, j1, j2 = datasets.spatialSubset(np.sort(lat)[::-1], np.sort(lon), res, bbox)
        t = pds.variables["time"]
        tt = netcdf.num2date(t[:], units=t.units)
        ti = [tj for tj in range(len(tt)) if resetDatetime(tt[tj]) >= dt[0] and resetDatetime(tt[tj]) <= dt[1]]
        if len(ti) > 0:
            lati = np.argsort(lat)[::-1][i1:i2]
            loni = np.argsort(lon)[j1:j2]
            if data is None:
                data = pds.variables[dsvar[ui]][ti, lati, loni]
            else:
                data = np.sqrt(
                    data ** 2.0 + pds.variables[dsvar[ui]][ti, lati, loni] ** 2.0)
            if any(tvar in dsvar for tvar in ["temp", "tmax", "tmin"]): 
                data -= 273.15
        lat = np.sort(lat)[::-1][i1:i2]
        lon = np.sort(lon)[j1:j2]
    table = "{0}.ncep".format(varname)
    for t in range(len(ti)):
        if not os.path.isdir("{0}/{1}/ncep".format(rpath.data, varname)):
            os.makedirs("{0}/{1}/ncep".format(rpath.data, varname))
        filename = "{0}/{1}/ncep/ncep_{2}.tif".format(rpath.data, varname, tt[ti[t]].strftime("%Y%m%d"))
        dbio.writeGeotif(lat, lon, res, data[t, :, :], filename)
        dbio.ingest(dbname, filename, tt[ti[t]], table)
        os.remove(filename)
    for dtt in [dt[0] + timedelta(days=tj) for tj in range((dt[-1]-dt[0]).days + 1)]:
        if dtt not in tt:
            log.warning("NCEP data not available for {0}. Skipping download!".format(
                dtt.strftime("%Y-%m-%d")))


def download(dbname, dts, bbox=None):
    """Downloads NCEP Reanalysis data from the IRI data server,
    and imports them into the database *db*. Optionally uses a bounding box to
    limit the region with [minlon, minlat, maxlon, maxlat]."""
    # for dt in [dts[0] + timedelta(tt) for tt in range((dts[1] - dts[0]).days + 1)]:
    years = range(dts[0].year, dts[-1].year + 1)
    for yr in years:
        dt = [max(datetime(yr, 1, 1), dts[0]), min(datetime(yr, 12, 31), dts[-1])]
        for varname in ["tmax", "tmin", "wind"]:
            _downloadVariable(varname, dbname, dt, bbox)
