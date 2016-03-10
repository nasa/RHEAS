""" Definition for RHEAS Datasets decorators.

.. module:: datasets.decorators
   :synopsis: Definition of the Datasets decorators

.. moduleauthor:: Kostas Andreadis <kandread@jpl.nasa.gov>

"""

from functools import wraps
import netCDF4 as netcdf4
import numpy as np
import tempfile
import shutil
import urllib
import zipfile
import gzip
from osgeo import gdal
from datetime import timedelta, datetime
import datasets


def _resetDatetime(dt):
    """Set time to 00:00 to align with daily data."""
    return datetime(dt.year, dt.month, dt.day, 0, 0)


def netcdf(fetch):
    """Decorator for fetching NetCDF files (local or from Opendap servers)."""
    @wraps(fetch)
    def wrapper(*args, **kwargs):
        url, varname, bbox, dt = fetch(*args, **kwargs)
        ds = netcdf4.Dataset(url)
        for var in ds.variables:
            if var.lower().startswith("lon") or var.lower() == "x":
                lonvar = var
            if var.lower().startswith("lat") or var.lower() == "y":
                latvar = var
            if var.lower().startswith("time") or var.lower() == "t":
                timevar = var
        lat = ds.variables[latvar][:]
        lon = ds.variables[lonvar][:]
        lon[lon > 180] -= 360
        res = abs(lat[0]-lat[1])  # assume rectangular grid
        i1, i2, j1, j2 = datasets.spatialSubset(lat, lon, res, bbox)
        lat = lat[i1:i2]
        lon = lon[j1:j2]
        t = ds.variables[timevar]
        tt = netcdf4.num2date(t[:], units=t.units)
        ti = [tj for tj in range(len(tt)) if _resetDatetime(tt[tj]) >= dt[
            0] and _resetDatetime(tt[tj]) <= dt[1]]
        print dt, ti
        if len(ti) > 0:
            tdata = ds.variables[varname][ti, i1:i2, j1:j2]
            lati = np.argsort(lat)[::-1]
            loni = np.argsort(lon)
            data = np.zeros((len(ti), len(lat), len(lon)))
            for i in range(len(lat)):
                for j in range(len(lon)):
                    data[:, i, j] = tdata[:, lati[i], loni[j]]
            dts = tt[ti]
        else:
            data = None
            dts = None
        return data, lat, lon, dts
    return wrapper


def geotiff(fetch):
    """Decorator for fetching Geotiff files."""
    @wraps(fetch)
    def wrapper(*args, **kwargs):
        url, bbox, dt = fetch(*args, **kwargs)
        outpath = tempfile.mkdtemp()
        data = []
        ts = []
        for tt in range((dt[1] - dt[0]).days + 1):
            t = dt[0] + timedelta(tt)
            ts.append(t)
            filename = url.format(t.year, t.month, t.day)
            lfilename = filename.split("/")[-1]
            if filename.find("http") >= 0 or filename.find("ftp") >= 0:
                urllib.urlretrieve(
                    filename, "{0}/{1}".format(outpath, lfilename))
            if lfilename.endswith("gz"):
                f = gzip.open("{0}/{1}".format(outpath, lfilename), 'rb')
                contents = f.read()
                f.close()
                lfilename = lfilename.replace(".gz", "")
                with open("{0}/{1}".format(outpath, lfilename), 'wb') as f:
                    f.write(contents)
            elif lfilename.endswith("zip"):
                f = zipfile.ZipFile("{0}/{1}".format(outpath, lfilename))
                lfilename = filter(
                    lambda s: s.endswith("tif"), f.namelist())[0]
                f.extract(lfilename, outpath)
            f = gdal.Open("{0}/{1}".format(outpath, lfilename))
            xul, xres, _, yul, _, yres = f.GetGeoTransform()
            fdata = f.ReadAsArray()
            nr, nc = fdata.shape
            lat = np.arange(yul, yul + yres * nr, yres)
            lon = np.arange(xul, xul + xres * nc, xres)
            i1, i2, j1, j2 = datasets.spatialSubset(lat, lon, xres, bbox)
            lat = lat[i1:i2]
            lon = lon[j1:j2]
            data.append(fdata[i1:i2, j1:j2])
        data = np.array(data)
        shutil.rmtree(outpath)
        return data, lat, lon, np.array(ts)
    return wrapper
