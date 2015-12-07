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
from datetime import timedelta


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
        if bbox is not None:
            i = np.where(np.logical_and(lat > bbox[1], lat < bbox[3]))[0]
            j = np.where(np.logical_and(lon > bbox[0], lon < bbox[2]))[0]
            lat = lat[i]
            lon = lon[j]
        else:
            i = range(len(lat))
            j = range(len(lon))
        t = ds.variables[timevar]
        tt = netcdf4.num2date(t[:], units=t.units)
        ti = [tj for tj in range(len(tt)) if tt[tj] >= dt[
            0] and tt[tj] <= dt[1]]
        tdata = ds.variables[varname][ti, i[0]:i[-1] + 1, j[0]:j[-1] + 1]
        lati = np.argsort(lat)[::-1]
        loni = np.argsort(lon)
        data = np.zeros((len(ti), len(lat), len(lon)))
        for i in range(len(lat)):
            for j in range(len(lon)):
                data[:, i, j] = tdata[:, lati[i], loni[j]]
        return data, lat, lon, tt[ti]
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
            if bbox is not None:
                i = np.where(np.logical_and(lat > bbox[1], lat < bbox[3]))[0]
                j = np.where(np.logical_and(lon > bbox[0], lon < bbox[2]))[0]
                lat = lat[i]
                lon = lon[j]
            else:
                i = range(len(lat))
                j = range(len(lon))
            data.append(fdata[i[0]:i[-1] + 1, j[0]:j[-1] + 1])
        data = np.array(data)
        shutil.rmtree(outpath)
        return data, lat, lon, np.array(ts)
    return wrapper
