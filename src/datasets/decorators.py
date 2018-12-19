""" Definition for RHEAS Datasets decorators.

.. module:: datasets.decorators
   :synopsis: Definition of the Datasets decorators

.. moduleauthor:: Kostas Andreadis <kandread@jpl.nasa.gov>

"""

from functools import wraps
import tempfile
import shutil
import urllib
from datetime import datetime
from ftplib import FTP
import re
from pydap.client import open_url
import netCDF4 as netcdf4
import numpy as np
from osgeo import gdal
import datasets


def resetDatetime(dt):
    """Set time to 00:00 to align with daily data."""
    return datetime(dt.year, dt.month, dt.day, 0, 0)


def path(fetch):
    """Decorator for getting files from local path."""
    @wraps(fetch)
    def wrapper(*args, **kwargs):
        url, bbox, dt = fetch(*args, **kwargs)
        outpath = tempfile.mkdtemp()
        filename = url.format(dt.year, dt.month, dt.day)
        try:
            shutil.copy(filename, outpath)
            lfilename = filename.split("/")[-1]
        except:
            lfilename = None
        return outpath, lfilename, bbox, dt
    return wrapper


def http(fetch):
    """Decorator for downloading files from HTTP sites."""
    @wraps(fetch)
    def wrapper(*args, **kwargs):
        url, bbox, dt = fetch(*args, **kwargs)
        outpath = tempfile.mkdtemp()
        filename = url.format(dt.year, dt.month, dt.day)
        try:
            lfilename = filename.split("/")[-1]
            urllib.urlcleanup()
            urllib.urlretrieve(filename, "{0}/{1}".format(outpath, lfilename))
        except:
            lfilename = None
        return outpath, lfilename, bbox, dt
    return wrapper


def ftp(fetch):
    """Decorator for downloading files from FTP sites."""
    @wraps(fetch)
    def wrapper(*args, **kwargs):
        url, bbox, dt = fetch(*args, **kwargs)
        ftpurl = url.split("/")[2]
        outpath = tempfile.mkdtemp()
        try:
            conn = FTP(ftpurl)
            conn.login()
            conn.cwd("/".join(url.split("/")[3:-1]).format(dt.year, dt.month, dt.day))
            name = url.split("/")[-1].format(dt.year, dt.month, dt.day)
            filenames = [f for f in conn.nlst() if re.match(r".*{0}.*".format(name), f) is not None]
            if len(filenames) > 0:
                filename = filenames[0]
                with open("{0}/{1}".format(outpath, filename), 'wb') as f:
                    conn.retrbinary("RETR {0}".format(filename), f.write)
                filenames.append("{0}/{1}".format(outpath, filename))
            else:
                filename = None
        except:
            filename = None
        return outpath, filename, bbox, dt
    return wrapper


def opendap(fetch):
    """Decorator for fetching data from Opendap servers."""
    @wraps(fetch)
    def wrapper(*args, **kwargs):
        url, varname, bbox, dt = fetch(*args, **kwargs)
        ds = open_url(url)
        for var in ds.keys():
            if var.lower().startswith("lon") or var.lower() == "x":
                lonvar = var
            if var.lower().startswith("lat") or var.lower() == "y":
                latvar = var
            if var.lower().startswith("time") or var.lower() == "t":
                timevar = var
        lat = ds[latvar][:].data
        lon = ds[lonvar][:].data
        lon[lon > 180] -= 360
        res = np.mean(abs(np.diff(lat)))  # assume rectangular grid
        lati = np.argsort(lat)[-1::-1]  # ensure north-south orientation in array
        loni = np.argsort(lon)  # ensure west-east orientation in array
        i1, i2, j1, j2 = datasets.spatialSubset(lat[lati], lon[loni], res, bbox)
        lat = lat[lati][i1:i2+1]
        lon = lon[loni][j1:j2+1]
        t = ds[timevar]
        tt = netcdf4.num2date(t[:].data, units=t.units)
        ti = [tj for tj in range(len(tt)) if resetDatetime(tt[tj]) >= dt[0] and resetDatetime(tt[tj]) <= dt[1]]
        if len(ti) > 0:
            if lati[i1] > lati[i2]:
                i1, i2 = i2, i1
                nsflip = True
            else:
                nsflip = False
            if loni[j1] > loni[j2]:
                j1, j2 = j2, j1
                ewflip = True
            else:
                ewflip = False
            if len(ds[varname].data[0].shape) > 3:
               data = ds[varname].data[0][ti[0]:ti[-1]+1, 0, lati[i1]:lati[i2]+1, loni[j1]:loni[j2]+1]
            else:
                data = ds[varname].data[0][ti[0]:ti[-1]+1, lati[i1]:lati[i2]+1, loni[j1]:loni[j2]+1]
            if nsflip:
                data = np.flip(data, axis=1)
            if ewflip:
                data = np.flip(data, axis=2)
            dt = tt[ti]
        else:
            data = None
        return data, lat, lon, dt
    return wrapper
     
        
        
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
        res = np.mean(abs(np.diff(lat)))  # assume rectangular grid
        lati = np.argsort(lat)[-1::-1]  # ensure north-south orientation in array
        loni = np.argsort(lon)  # ensure west-east orientation in array
        i1, i2, j1, j2 = datasets.spatialSubset(np.sort(lat)[::-1], np.sort(lon), res, bbox)
        lat = lat[lati][i1:i2+1]
        lon = lon[loni][j1:j2+1]
        t = ds.variables[timevar]
        tt = netcdf4.num2date(t[:], units=t.units)
        ti = [tj for tj in range(len(tt)) if resetDatetime(tt[tj]) >= dt[0] and resetDatetime(tt[tj]) <= dt[1]]
        if len(ti) > 0:
            if lati[i1] > lati[i2]:
                i1, i2 = i2, i1
                nsflip = True
            else:
                nsflip = False
            if loni[j1] > loni[j2]:
                j1, j2 = j2, j1
                ewflip = True
            else:
                ewflip = False
            if len(ds.variables[varname].shape) > 3:
                data = ds.variables[varname][ti, 0, lati[i1]:lati[i2]+1, loni[j1]:loni[j2]+1]
            else:
                data = ds.variables[varname][ti, lati[i1]:lati[i2]+1, loni[j1]:loni[j2]+1]
            dt = tt[ti]
            if nsflip:
                data = np.flip(data, axis=1)
            if ewflip:
                data = np.flip(data, axis=2)
        else:
            data = None
        return data, lat, lon, dt
    return wrapper


def geotiff(fetch):
    """Decorator for reading data from raster files."""
    @wraps(fetch)
    def wrapper(*args, **kwargs):
        outpath, filename, bbox, dt = fetch(*args, **kwargs)
        if filename is not None:
            lfilename = datasets.uncompress(filename, outpath)
            f = gdal.Open("{0}/{1}".format(outpath, lfilename))
            xul, xres, _, yul, _, yres = f.GetGeoTransform()
            data = f.ReadAsArray()
            nr, nc = data.shape
            lat = np.arange(yul + yres/2.0, yul + yres * nr, yres)
            lon = np.arange(xul + xres/2.0, xul + xres * nc, xres)
            i1, i2, j1, j2 = datasets.spatialSubset(lat, lon, xres, bbox)
            data = data[i1:i2, j1:j2]
            lat = lat[i1:i2]
            lon = lon[j1:j2]
            shutil.rmtree(outpath)
        else:
            data = lat = lon = None
        return data, lat, lon, dt
    return wrapper
