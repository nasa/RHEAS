"""Class definition for the MODIS snow cover fraction data type.

.. module:: mod10
   :synopsis: Definition of the MOD10 class

.. moduleauthor:: Kostas Andreadis <kandread@jpl.nasa.gov>

"""

import dbio
from datetime import timedelta
import datasets
from ftplib import FTP
import tempfile
import subprocess
import glob
import shutil
import modis


table = "snow.mod10"


def dates(dbname):
    dts = datasets.dates(dbname, table)
    return dts


def download(dbname, dts, bbox):
    """Downloads the Terra MODIS snow cover fraction data product MOD10 for
    a specific date *dt* and imports them into the PostGIS database *dbname*."""
    res = 0.005
    url = "n5eil01u.ecs.nsidc.org"
    tiles = modis.findTiles(bbox)
    if tiles is not None:
        ftp = FTP(url)
        ftp.login()
        for dt in [dts[0] + timedelta(dti) for dti in range((dts[-1] - dts[0]).days + 1)]:
            try:
                ftp.cwd("SAN/MOST/MOD10A1.005/{1:04d}.{2:02d}.{3:02d}".format(url, dt.year, dt.month, dt.day))
                files = [f for f in ftp.nlst() if any(
                    f.find("h{0:02d}v{1:02d}".format(t[1], t[0])) > 0 for t in tiles)]
                files = filter(lambda s: s.endswith("hdf"), files)
                outpath = tempfile.mkdtemp()
                for fname in files:
                        with open("{0}/{1}".format(outpath, fname), 'wb') as f:
                            ftp.retrbinary("RETR {0}".format(fname), f.write)
                        subprocess.call(["gdal_translate", "HDF4_EOS:EOS_GRID:{0}/{1}:MOD_Grid_Snow_500m:Fractional_Snow_Cover".format(
                            outpath, fname), "{0}/{1}".format(outpath, fname).replace("hdf", "tif")])
                tifs = glob.glob("{0}/*.tif".format(outpath))
                subprocess.call(
                    ["gdal_merge.py", "-a_nodata", "-9999", "-o", "{0}/snow.tif".format(outpath)] + tifs)
                cmd = " ".join(["gdal_calc.py", "-A", "{0}/snow.tif".format(outpath), "--outfile={0}/snow1.tif".format(
                    outpath), "--NoDataValue=-9999", "--calc=\"(A<101.0)*(A+9999.0)-9999.0\""])
                subprocess.call(cmd, shell=True)
                cmd = " ".join(["gdalwarp", "-t_srs", "'+proj=latlong +ellps=sphere'", "-tr", str(
                    res), str(-res), "{0}/snow1.tif".format(outpath), "{0}/snow2.tif".format(outpath)])
                subprocess.call(cmd, shell=True)
                subprocess.call(["gdal_translate", "-a_srs", "epsg:4326",
                                 "{0}/snow2.tif".format(outpath), "{0}/snow3.tif".format(outpath)])
                dbio.ingest(
                    dbname, "{0}/snow3.tif".format(outpath), dt, table, False)
                shutil.rmtree(outpath)
            except:
                print("MOD10 data not available for {0}. Skipping download!".format(
                    dt.strftime("%Y-%m-%d")))


class Mod10(object):

    def __init__(self):
        """Initialize MOD10 snow cover fraction object."""
        self.statevar = ["swq"]
        self.obsvar = "snow_cover"
        self.res = 0.005
        self.stddev = 0.05
        self.tablename = table
