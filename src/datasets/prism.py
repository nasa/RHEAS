""" RHEAS module for retrieving PRISM meteorological data.

.. module:: prism
   :synopsis: Retrieve PRISM meteorological data

.. moduleauthor:: Kostas Andreadis <kandread@jpl.nasa.gov>

"""


from ftplib import FTP
import datasets
from datetime import datetime
import tempfile
import subprocess
import zipfile
import dbio
import os
import rpath


table = {"ppt": "precip.prism", "tmax": "tmax.prism", "tmin": "tmin.prism"}


def dates(dbname):
    dts = datasets.dates(dbname, table['ppt'])
    return dts


def _downloadVariable(varname, dbname, dts, bbox):
    """Downloads the PRISM data products for a specific variable and a set of
    dates *dt*. *varname* can be ppt, tmax or tmin."""
    url = "prism.oregonstate.edu"
    ftp = FTP(url)
    ftp.login()
    ftp.cwd("daily/{0}".format(varname))
    outpath = tempfile.mkdtemp()
    years = list(set([t.year for t in dts]))
    for yr in years:
        ftp.cwd("{0}".format(yr))
        filenames = [f for f in ftp.nlst() if datetime.strptime(f.split("_")[-2], "%Y%m%d") >= dts[0] and datetime.strptime(f.split("_")[-2], "%Y%m%d") <= dts[-1]]
        for fname in filenames:
            dt = datetime.strptime(fname.split("_")[-2], "%Y%m%d")
            with open("{0}/{1}".format(outpath, fname), 'wb') as f:
                ftp.retrbinary("RETR {0}".format(fname), f.write)
            if fname.endswith("zip"):
                fz = zipfile.ZipFile("{0}/{1}".format(outpath, fname))
                lfilename = filter(lambda s: s.endswith("bil"), fz.namelist())[0]
                fz.extractall(outpath)
            else:
                lfilename = fname
            if bbox is None:
                pstr = []
            else:
                pstr = ["-projwin", str(bbox[0]), str(bbox[3]), str(bbox[2]), str(bbox[1])]
            if not os.path.isdir("{0}/precip/prism".format(rpath.data)):
                    os.mkdir("{0}/precip/prism".format(rpath.data))
            filename = "{0}/precip/prism/prism_{1}.tif".format(rpath.data, dt.strftime("%Y%m%d"))
            subprocess.call(["gdal_translate", "-a_srs", "epsg:4326"] + pstr + ["{0}/{1}".format(outpath, lfilename), filename])
            dbio.ingest(dbname, filename, dt, table)
        ftp.cwd("..")


def download(dbname, dts, bbox):
    """Downloads the PRISM data products for a set of
    dates *dt* and imports them into the PostGIS database *dbname*."""
    for varname in ["ppt", "tmax", "tmin"]:
        _downloadVariable(varname, dbname, dts, bbox)
