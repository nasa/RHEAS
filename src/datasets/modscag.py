"""Class definition for the MODSCAG snow cover fraction data type.

.. module:: modscag
   :synopsis: Definition of the MODSCAG class

.. moduleauthor:: Kostas Andreadis <kandread@jpl.nasa.gov>

"""

from snowcover import Snowcover
import dbio
from datetime import timedelta
import datasets
import modis
import requests
from requests.auth import HTTPDigestAuth
import tempfile
import lxml.html
import subprocess
import glob
import shutil
import logging


table = "snow.modscag"
username = "akostas"
password = "m0red@7@!"


def dates(dbname):
    dts = datasets.dates(dbname, table)
    return dts


def download(dbname, dts, bbox):
    """Downloads the MODSCAG snow cover fraction data product for a specific
    date *dt* and imports it into the PostGIS database *dbname*."""
    log = logging.getLogger(__name__)
    res = 0.01
    tiles = modis.findTiles(bbox)
    for dt in [dts[0] + timedelta(dti) for dti in range((dts[-1] - dts[0]).days + 1)]:
        temppath = tempfile.mkdtemp()
        url = "https://snow-data.jpl.nasa.gov/modscag-historic/{0}/{1}".format(dt.year, dt.strftime("%j"))
        r = requests.get(url, auth=HTTPDigestAuth(username, password))
        if r.status_code == 200:
            dom = lxml.html.fromstring(r.text)
            links = [link for link in dom.xpath('//a/@href') if link.find("snow_fraction.tif") > 0]
            for t in tiles:
                filenames = filter(lambda f: f.find("h{0:02d}v{1:02d}".format(t[1], t[0])) > 0, links)
                if len(filenames) > 0:
                    filename = filenames[0]
                    r = requests.get("{0}/{1}".format(url, filename), auth=HTTPDigestAuth(username, password))
                    with open("{0}/{1}".format(temppath, filename), 'wb') as fout:
                        fout.write(r.content)
            tifs = glob.glob("{0}/*.tif".format(temppath))
            if len(tifs) > 0:
                proc = subprocess.Popen(["gdal_merge.py", "-o", "{0}/snow.tif".format(temppath)] + tifs, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                out, err = proc.communicate()
                log.debug(out)
                proc = subprocess.Popen(["gdal_calc.py", "-A", "{0}/snow.tif".format(temppath), "--outfile={0}/snow1.tif".format(
                    temppath), "--NoDataValue=-9999", "--calc=\"(A<101.0)*(A+9999.0)-9999.0\""], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                out, err = proc.communicate()
                log.debug(out)
                proc = subprocess.Popen(["gdalwarp", "-t_srs", "'+proj=latlong +ellps=sphere'", "-tr", str(
                    res), str(-res), "{0}/snow1.tif".format(temppath), "{0}/snow2.tif".format(temppath)], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                out, err = proc.communicate()
                log.debug(out)
                if bbox is None:
                    pstr = []
                else:
                    pstr = ["-projwin", str(bbox[0]), str(bbox[3]), str(bbox[2]), str(bbox[1])]
                proc = subprocess.Popen(["gdal_translate", "-a_srs", "epsg:4326"] + pstr + ["{0}/snow2.tif".format(temppath), "{0}/snow3.tif".format(temppath)], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                out, err = proc.communicate()
                log.debug(out)
                dbio.ingest(dbname, "{0}/snow3.tif".format(temppath), dt, table, False)
                shutil.rmtree(temppath)
            else:
                log.warning("MODSCAG data not available for {0}. Skipping download!".format(
                    dt.strftime("%Y-%m-%d")))


class Modscag(Snowcover):

    def __init__(self, uncert=None):
        super(Modscag, self).__init__(uncert)
        self.res = 0.01
        self.stddev = 0.05
        self.tablename = "snow.modscag"
