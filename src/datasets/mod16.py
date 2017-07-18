""" RHEAS module for retrieving MODIS evapotranspiration data (MOD16 product).

.. module:: mod16
   :synopsis: Retrieve MODIS evapotranspiration data

.. moduleauthor:: Kostas Andreadis <kandread@jpl.nasa.gov>

"""

import modis
import tempfile
import subprocess
import glob
import shutil
import dbio
import datasets
from datetime import timedelta
import requests
from bs4 import BeautifulSoup, SoupStrainer
from bs4.element import Tag
import logging


table = "evap.mod16"


def dates(dbname):
    dts = datasets.dates(dbname, table)
    return dts


def download(dbname, dts, bbox):
    """Downloads the MODIS evapotranspiration data product MOD16 for
    a set of dates *dt* and imports them into the PostGIS database *dbname*."""
    log = logging.getLogger(__name__)
    res = 0.01
    urlbase = "http://files.ntsg.umt.edu"
    tiles = modis.findTiles(bbox)
    if tiles is not None:
        for dt in [dts[0] + timedelta(dti) for dti in range((dts[-1] - dts[0]).days + 1)]:
            url = "{0}/data/NTSG_Products/MOD16/MOD16A2.105_MERRAGMAO/Y{1}".format(urlbase, dt.year)
            resp_year = requests.get(url)
            try:
                assert resp_year.status_code == 200
                days = [link for link in BeautifulSoup(resp_year.text, parse_only=SoupStrainer('a')) if isinstance(link, Tag) and link.text.find(dt.strftime("%j")) >= 0]
                assert len(days) > 0
                resp_day = requests.get("{0}{1}".format(urlbase, days[0].get('href')))
                assert resp_day.status_code == 200
                files = [link.get('href') for link in BeautifulSoup(resp_day.text, parse_only=SoupStrainer('a')) if isinstance(link, Tag) and link.text.find("hdf") > 0]
                files = [f for f in files if any(f.find("h{0:02d}v{1:02d}".format(t[1], t[0])) > 0 for t in tiles)]
                outpath = tempfile.mkdtemp()
                for fname in files:
                    resp_file = requests.get("{0}{1}".format(urlbase, fname)) 
                    filename = fname.split("/")[-1]
                    with open("{0}/{1}".format(outpath, filename), 'wb') as fout:
                        for chunk in resp_file:
                            fout.write(chunk)
                    proc = subprocess.Popen(["gdal_translate", "HDF4_EOS:EOS_GRID:{0}/{1}:MOD_Grid_MOD16A2:ET_1km".format(
                        outpath, filename), "{0}/{1}".format(outpath, filename).replace("hdf", "tif")], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                    out, err = proc.communicate()
                    log.debug(out)
                tifs = glob.glob("{0}/*.tif".format(outpath))
                proc = subprocess.Popen(
                    ["gdal_merge.py", "-o", "{0}/et.tif".format(outpath)] + tifs, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                out, err = proc.communicate()
                log.debug(out)
                proc = subprocess.Popen(["gdal_calc.py", "-A", "{0}/et.tif".format(outpath), "--outfile={0}/et1.tif".format(
                    outpath), "--NoDataValue=-9999", "--calc=(A<32701)*(0.1*A+9999)-9999"], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                out, err = proc.communicate()
                log.debug(out)
                proc = subprocess.Popen(["gdalwarp", "-t_srs", "+proj=latlong +ellps=sphere", "-tr", str(
                    res), str(-res), "{0}/et1.tif".format(outpath), "{0}/et2.tif".format(outpath)], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                out, err = proc.communicate()
                log.debug(out)
                if bbox is None:
                    pstr = []
                else:
                    pstr = ["-projwin", str(bbox[0]), str(bbox[3]), str(bbox[2]), str(bbox[1])]
                proc = subprocess.Popen(["gdal_translate"] + pstr + ["-a_srs", "epsg:4326", "{0}/et2.tif".format(outpath), "{0}/et3.tif".format(outpath)], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                out, err = proc.communicate()
                log.debug(out)
                dbio.ingest(
                    dbname, "{0}/et3.tif".format(outpath), dt, table, False)
                shutil.rmtree(outpath)
            except:
                log.warning("MOD16 data not available for {0}. Skipping download!".format(
                    dt.strftime("%Y-%m-%d")))


class Mod16:

    def __init__(self):
        """Initialize MOD16 evapotranspiration object."""
        self.statevar = ["soil_moist"]
        self.obsvar = "evap"
        self.res = 0.01
        self.stddev = 1.0
        self.tablename = "evap.mod16"
