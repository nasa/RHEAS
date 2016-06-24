"""Class definition for the MODSCAG snow cover fraction data type.

.. module:: modscag
   :synopsis: Definition of the MODSCAG class

.. moduleauthor:: Kostas Andreadis <kandread@jpl.nasa.gov>

"""

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
import numpy as np


table = "snow.modscag"
username = "akostas"
password = "m0red@7@!"


def dates(dbname):
    dts = datasets.dates(dbname, table)
    return dts


def download(dbname, dts, bbox):
    """Downloads the MODSCAG snow cover fraction data product for a specific
    date *dt* and imports it into the PostGIS database *dbname*."""
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
                subprocess.call(["gdal_merge.py", "-o", "{0}/snow.tif".format(temppath)] + tifs)
                cmd = " ".join(["gdal_calc.py", "-A", "{0}/snow.tif".format(temppath), "--outfile={0}/snow1.tif".format(
                    temppath), "--NoDataValue=-9999", "--calc=\"(A<101.0)*(A+9999.0)-9999.0\""])
                subprocess.call(cmd, shell=True)
                cmd = " ".join(["gdalwarp", "-t_srs", "'+proj=latlong +ellps=sphere'", "-tr", str(
                    res), str(-res), "{0}/snow1.tif".format(temppath), "{0}/snow2.tif".format(temppath)])
                subprocess.call(cmd, shell=True)
                if bbox is None:
                    pstr = []
                else:
                    pstr = ["-projwin", str(bbox[0]), str(bbox[3]), str(bbox[2]), str(bbox[1])]
                subprocess.call(["gdal_translate", "-a_srs", "epsg:4326"] + pstr + ["{0}/snow2.tif".format(temppath), "{0}/snow3.tif".format(temppath)])
                dbio.ingest(dbname, "{0}/snow3.tif".format(temppath), dt, table, False)
                shutil.rmtree(temppath)
            else:
                print("MODSCAG data not available for {0}. Skipping download!".format(
                    dt.strftime("%Y-%m-%d")))


class Modscag(object):

    def __init__(self):
        """Initialize MODSCAG snow cover fraction object."""
        self.statevar = ["swq"]
        self.obsvar = "snow_cover"
        self.res = 0.01
        self.stddev = 0.05
        self.tablename = table

    def x(self, dt, models):
        """Retrieve state variable from database."""
        data = {}
        db = dbio.connect(models.dbname)
        cur = db.cursor()
        for s in self.statevar:
            sql = "select ensemble,st_x(geom),st_y(geom),val from (select ensemble,(ST_PixelAsCentroids(rast)).* from {0}.{1} where fdate=date '{2}-{3}-{4}') foo group by ensemble,geom order by ensemble".format(
                models.name, s, dt.year, dt.month, dt.day)
            cur.execute(sql)
            e, lon, lat, vals = zip(*cur.fetchall())
            gid = [models[0].lgid[(l[0], l[1])] for l in zip(lat, lon)]
            nens = max(e)
            data[s] = np.array(vals).reshape((len(vals) / nens, nens))
            lat = np.array(lat).reshape((len(lat) / nens, nens))
            lon = np.array(lon).reshape((len(lon) / nens, nens))
            gid = np.array(gid).reshape((len(gid) / nens, nens))
        cur.close()
        db.close()
        return data, lat, lon, gid

    def get(self, dt, models):
        """Retrieve observations from database for date *dt*."""
        db = dbio.connect(models.dbname)
        cur = db.cursor()
        sql = "select st_x(geom),st_y(geom),val from (select (st_pixelascentroids(st_clip(rast,geom))).* from {0},{1}.basin where st_intersects(rast,geom) and fdate=date '{2}-{3}-{4}') foo".format(
            self.tablename, models.name, dt.year, dt.month, dt.day)
        cur.execute(sql)
        if bool(cur.rowcount):
            lon, lat, data = zip(*cur.fetchall())
            data = np.array(data).reshape((len(data), 1))
            lat = np.array(lat).reshape((len(lat), 1))
            lon = np.array(lon).reshape((len(lon), 1))
            self.nobs = len(data)
        else:
            data = lat = lon = None
        cur.close()
        db.close()
        return data, lat, lon
