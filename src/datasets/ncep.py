""" RHEAS module for retrieving maximum and minimum
temperature from the NCEP Reanalysis stored at the IRI Data Library.

.. module:: ncep
   :synopsis: Retrieve NCEP meteorological data

.. moduleauthor:: Kostas Andreadis <kandread@jpl.nasa.gov>

"""

import numpy as np
import datasets
from decorators import netcdf
from datetime import timedelta


def dates(dbname):
    dts = datasets.dates(dbname, "wind.ncep")
    return dts


@netcdf
def fetch_tmax(dbname, dt, bbox):
    """Downloads maximum temperature from NCEP Reanalysis."""
    url = "http://iridl.ldeo.columbia.edu/SOURCES/.NOAA/.NCEP-NCAR/.CDAS-1/.DAILY/.Diagnostic/.above_ground/.maximum/dods"
    varname = "temp"
    return url, varname, bbox, dt


@netcdf
def fetch_tmin(dbname, dt, bbox):
    """Downloads minimum temperature from NCEP Reanalysis."""
    url = "http://iridl.ldeo.columbia.edu/SOURCES/.NOAA/.NCEP-NCAR/.CDAS-1/.DAILY/.Diagnostic/.above_ground/.minimum/dods"
    varname = "temp"
    return url, varname, bbox, dt


@netcdf
def fetch_uwnd(dbname, dt, bbox):
    """Downloads U-component wind speed from NCEP Reanalysis."""
    url = "http://iridl.ldeo.columbia.edu/SOURCES/.NOAA/.NCEP-NCAR/.CDAS-1/.DAILY/.Diagnostic/.above_ground/.u/dods"
    varname = "u"
    return url, varname, bbox, dt


@netcdf
def fetch_vwnd(dbname, dt, bbox):
    """Downloads U-component wind speed from NCEP Reanalysis."""
    url = "http://iridl.ldeo.columbia.edu/SOURCES/.NOAA/.NCEP-NCAR/.CDAS-1/.DAILY/.Diagnostic/.above_ground/.v/dods"
    varname = "v"
    return url, varname, bbox, dt


def download(dbname, dts, bbox=None):
    """Downloads NCEP Reanalysis data from IRI data library."""
    res = 1.875
    tmax, lat, lon, _ = fetch_tmax(dbname, dts, bbox)
    tmin, _, _, _ = fetch_tmin(dbname, dts, bbox)
    uwnd, _, _, _ = fetch_uwnd(dbname, dts, bbox)
    vwnd, _, _, dts = fetch_vwnd(dbname, dts, bbox)
    wnd = np.sqrt(uwnd**2 + vwnd**2)
    tmax -= 273.15
    tmin -= 273.15
    for t, dt in enumerate([dts[0] + timedelta(tt) for tt in range((dts[-1] - dts[0]).days + 1)]):
        datasets.ingest(dbname, "tmax.ncep", tmax[t, :, :], lat, lon, res, dt)
        datasets.ingest(dbname, "tmin.ncep", tmin[t, :, :], lat, lon, res, dt)
        datasets.ingest(dbname, "wind.ncep", wnd[t, :, :], lat, lon, res, dt)
