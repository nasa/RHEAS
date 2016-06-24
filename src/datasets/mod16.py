""" RHEAS module for retrieving MODIS evapotranspiration data (MOD16 product).

.. module:: mod16
   :synopsis: Retrieve MODIS evapotranspiration data

.. moduleauthor:: Kostas Andreadis <kandread@jpl.nasa.gov>

"""

import modis
import tempfile
from ftplib import FTP
import subprocess
import glob
import shutil
import dbio
import datasets
from datetime import timedelta


table = "evap.mod16"


def dates(dbname):
    dts = datasets.dates(dbname, table)
    return dts


def download(dbname, dts, bbox):
    """Downloads the MODIS evapotranspiration data product MOD16 for
    a set of dates *dt* and imports them into the PostGIS database *dbname*."""
    res = 0.01
    url = "ftp.ntsg.umt.edu"
    tiles = modis.findTiles(bbox)
    if tiles is not None:
        ftp = FTP(url)
        ftp.login()
        for dt in [dts[0] + timedelta(dti) for dti in range((dts[-1] - dts[0]).days + 1)]:
            try:
                ftp.cwd(
                    "pub/MODIS/NTSG_Products/MOD16/MOD16A2.105_MERRAGMAO/Y{0}".format(dt.year))
                days = ftp.nlst()
                datadir = "D{0}".format(dt.strftime("%j"))
                if datadir in days:
                    ftp.cwd(datadir)
                    files = [f for f in ftp.nlst() if any(
                        f.find("h{0:02d}v{1:02d}".format(t[1], t[0])) > 0 for t in tiles)]
                    outpath = tempfile.mkdtemp()
                    for fname in files:
                        with open("{0}/{1}".format(outpath, fname), 'wb') as f:
                            ftp.retrbinary("RETR {0}".format(fname), f.write)
                        subprocess.call(["gdal_translate", "HDF4_EOS:EOS_GRID:{0}/{1}:MOD_Grid_MOD16A2:ET_1km".format(
                            outpath, fname), "{0}/{1}".format(outpath, fname).replace("hdf", "tif")])
                    tifs = glob.glob("{0}/*.tif".format(outpath))
                    subprocess.call(
                        ["gdal_merge.py", "-o", "{0}/et.tif".format(outpath)] + tifs)
                    cmd = " ".join(["gdal_calc.py", "-A", "{0}/et.tif".format(outpath), "--outfile={0}/et1.tif".format(
                        outpath), "--NoDataValue=-9999", "--calc=\"(A<32701)*(0.1*A+9999)-9999\""])
                    subprocess.call(cmd, shell=True)
                    cmd = " ".join(["gdalwarp", "-t_srs", "'+proj=latlong +ellps=sphere'", "-tr", str(
                        res), str(-res), "{0}/et1.tif".format(outpath), "{0}/et2.tif".format(outpath)])
                    subprocess.call(cmd, shell=True)
                    if bbox is None:
                        pstr = []
                    else:
                        pstr = ["-projwin", str(bbox[0]), str(bbox[3]), str(bbox[2]), str(bbox[1])]
                    subprocess.call(["gdal_translate"] + pstr + ["-a_srs", "epsg:4326", "{0}/et2.tif".format(outpath), "{0}/et3.tif".format(outpath)])
                    dbio.ingest(
                        dbname, "{0}/et3.tif".format(outpath), dt, table, False)
                    shutil.rmtree(outpath)
            except:
                print("MOD16 data not available for {0}. Skipping download!".format(
                    dt.strftime("%Y-%m-%d")))


class Mod16:

    def __init__(self):
        """Initialize MOD16 evapotranspiration object."""
        self.statevar = ["soil_moist"]
        self.obsvar = "evap"
        self.res = 0.01
        self.stddev = 1.0
        self.tablename = "evap.mod16"
