""" RHEAS module for retrieving MODIS Leaf Area Index data (MCD15 product).

.. module:: mcd15
   :synopsis: Retrieve MODIS LAI data

.. moduleauthor:: Kostas Andreadis <kandread@jpl.nasa.gov>

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


table = "lai.modis"


def dates(dbname):
    dts = datasets.dates(dbname, table)
    return dts


def download(dbname, dts, bbox):
    """Downloads the combined MODIS LAI data product MCD15 for
    a specific date *dt* and imports them into the PostGIS database *dbname*."""
    log = logging.getLogger(__name__)
    res = 0.005
    burl = "http://e4ftl01.cr.usgs.gov/MOTA/MCD15A2H.006"
    tiles = modis.findTiles(bbox)
    if tiles is not None:
        for dt in [dts[0] + timedelta(dti) for dti in range((dts[-1] - dts[0]).days + 1)]:
            outpath = tempfile.mkdtemp()
            url = "{0}/{1:04d}.{2:02d}.{3:02d}".format(burl, dt.year, dt.month, dt.day)
            filenames = []
            for t in tiles:
                try:
                    tmppath, fname = earthdata.download(url, "MCD15A2H.A{0}.h{1:02d}v{2:02d}.006.*.hdf".format(dt.strftime("%Y%j"), t[1], t[0]))
                except ConnectionError:
                    fname = None
                if fname:
                    filenames.append("{0}/{1}".format(tmppath, fname))
            for filename in filenames:
                proc = subprocess.Popen(["gdal_translate", "HDF4_EOS:EOS_GRID:{0}:MOD_Grid_MOD15A2H:Lai_500m".format(
                                filename), "{0}/{1}".format(outpath, filename.split("/")[-1]).replace("hdf", "tif")], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                out, err = proc.communicate()
                log.debug(out)
                shutil.rmtree("/".join(filename.split("/")[:-1]))
            tifs = glob.glob("{0}/*.tif".format(outpath))
            if len(tifs) > 0:
                proc = subprocess.Popen(["gdal_merge.py", "-o", "{0}/lai.tif".format(outpath)] + tifs, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                out, err = proc.communicate()
                log.debug(out)
                proc = subprocess.Popen(["gdal_calc.py", "-A", "{0}/lai.tif".format(outpath), "--outfile={0}/lai1.tif".format(outpath), "--NoDataValue=-9999", "--calc=(A<101.0)*(0.1*A+9999.0)-9999.0"], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                out, err = proc.communicate()
                log.debug(out)
                proc = subprocess.Popen(["gdalwarp", "-t_srs", "+proj=latlong +ellps=sphere", "-tr", str(res), str(-res), "{0}/lai1.tif".format(outpath), "{0}/lai2.tif".format(outpath)], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                out, err = proc.communicate()
                log.debug(out)
                proc = subprocess.Popen(["gdal_translate", "-a_srs", "epsg:4326", "{0}/lai2.tif".format(outpath), "{0}/lai3.tif".format(outpath)], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                out, err = proc.communicate()
                log.debug(out)
                dbio.ingest(dbname, "{0}/lai3.tif".format(outpath), dt, table, False)
            else:
                log.warning("MCD15 data not available for {0}. Skipping download!".format(dt.strftime("%Y-%m-%d")))
            shutil.rmtree(outpath)
