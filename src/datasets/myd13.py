""" RHEAS module for retrieving MODIS NDVI data (MYD13 product).

.. module:: myd13
   :synopsis: Retrieve MODIS NDVI data

.. moduleauthor:: Kostas Andreadis <kandread@umass.edu>

"""

import modis
import tempfile
import dbio
import subprocess
import glob
import shutil
import datasets
from datetime import timedelta
from requests import ConnectionError
import logging
import earthdata


table = "ndvi.modis"


def dates(dbname):
    dts = datasets.dates(dbname, table)
    return dts


def download(dbname, dts, bbox):
    """Downloads the 5-km MODIS NDVI data product MYD13 for
    a date range *dts* and imports them into the PostGIS database *dbname*."""
    log = logging.getLogger(__name__)
    res = 0.05
    burl = "https://e4ftl01.cr.usgs.gov/MOLA/MYD13C1.006"
    for dt in [dts[0] + timedelta(dti) for dti in range((dts[-1] - dts[0]).days + 1)]:
        outpath = tempfile.mkdtemp()
        url = "{0}/{1:04d}.{2:02d}.{3:02d}".format(burl, dt.year, dt.month, dt.day)
        try:
            tmppath, fname = earthdata.download(url, "MYD13C1.A{0}.006.*.hdf".format(dt.strftime("%Y%j")))
        except ConnectionError:
            fname = None
        if fname:
            filename = "{0}/{1}".format(tmppath, fname)
            proc = subprocess.Popen(["gdal_translate", "HDF4_EOS:EOS_GRID:{0}:MODIS_Grid_16Day_VI_CMG:CMG 0.05 Deg 16 days NDVI".format(
                            filename), "{0}/ndvi.tif".format(outpath)], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            out, err = proc.communicate()
            log.debug(out)
            proc = subprocess.Popen(["gdal_calc.py", "-A", "{0}/ndvi.tif".format(outpath), "--outfile={0}/ndvi1.tif".format(outpath), "--NoDataValue=-9999", "--calc=(A>=-2000)*(0.0001*A+9999.0)-9999.0"], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            out, err = proc.communicate()
            log.debug(out)
            proc = subprocess.Popen(["gdalwarp", "-t_srs", "+proj=latlong +ellps=sphere", "-tr", str(res), str(-res), "{0}/ndvi1.tif".format(outpath), "{0}/ndvi2.tif".format(outpath)], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            out, err = proc.communicate()
            log.debug(out)
            if bbox is None:
                pstr = []
            else:
                pstr = ["-projwin", str(bbox[0]), str(bbox[3]), str(bbox[2]), str(bbox[1])]
            proc = subprocess.Popen(["gdal_translate"] + pstr + ["-a_srs", "epsg:4326", "{0}/ndvi2.tif".format(outpath), "{0}/ndvi3.tif".format(outpath)], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            out, err = proc.communicate()
            log.debug(out)
            dbio.ingest(dbname, "{0}/ndvi3.tif".format(outpath), dt, table, False)
        else:
            log.warning("MYD13 data not available for {0}. Skipping download!".format(dt.strftime("%Y-%m-%d")))
        shutil.rmtree(outpath)
