"""Class definition for the AMSR-E Soil Mositure data type.

.. module:: amsre
   :synopsis: Definition of the AMSRE class

.. moduleauthor:: Kostas Andreadis <kandread@jpl.nasa.gov>

"""

from soilmoist import Soilmoist
from ftplib import FTP
from datetime import timedelta
import tempfile
import subprocess
import datasets
import dbio


table = "soilmoist.amsre"


def dates(dbname):
    dts = datasets.dates(dbname, "soilmoist.amsre")
    return dts


def download(dbname, dts, bbox):
    """Downloads AMSR-E soil moisture data for a set of dates *dts*
    and imports them into the PostGIS database *outpath*. Optionally
    uses a bounding box to limit the region with [minlon, minlat, maxlon, maxlat]."""
    url = "n5eil01u.ecs.nsidc.org"
    ftp = FTP(url)
    ftp.login()
    ftp.cwd("SAN/AMSA/AE_Land3.002")
    for dt in [dts[0] + timedelta(ti) for ti in range((dts[-1] - dts[0]).days+1)]:
        datadir = dt.strftime("%Y.%m.%d")
        try:
            tmppath = tempfile.mkdtemp()
            ftp.cwd(datadir)
            fname = [f for f in ftp.nlst() if f.endswith("hdf")][0]
            with open("{0}/{1}".format(tmppath, fname), 'wb') as f:
                ftp.retrbinary("RETR {0}".format(fname), f.write)
            subprocess.call(["gdal_translate", "HDF4_EOS:EOS_GRID:{0}/{1}:Ascending_Land_Grid:A_Soil_Moisture".format(tmppath, fname), "{0}/sma.tif".format(tmppath)])
            subprocess.call(["gdal_translate", "HDF4_EOS:EOS_GRID:{0}/{1}:Descending_Land_Grid:D_Soil_Moisture".format(tmppath, fname), "{0}/smd.tif".format(tmppath)])
            # merge orbits
            subprocess.call(["gdal_merge.py", "-o", "{0}/sm1.tif".format(tmppath), "{0}/sma.tif".format(tmppath), "{0}/smd.tif".format(tmppath)])
            # reproject data
            subprocess.call(["gdalwarp", "-s_srs", "epsg:3410", "-t_srs", "epsg:4326", "{0}/sm1.tif".format(tmppath), "{0}/sm2.tif".format(tmppath)])
            if bbox is None:
                pstr = []
            else:
                pstr = ["-projwin", str(bbox[0]), str(bbox[3]), str(bbox[2]), str(bbox[1])]
            subprocess.call(["gdal_translate"] + pstr + ["-ot", "Float32", "{0}/sm2.tif".format(tmppath), "{0}/sm3.tif".format(tmppath)])
            filename = "{0}/amsre_soilm_{1}.tif".format(tmppath, dt.strftime("%Y%m%d"))
            cmd = " ".join(["gdal_calc.py", "-A", "{0}/sm3.tif".format(tmppath), "--outfile={0}".format(filename), "--NoDataValue=-9999", "--calc=\"(abs(A)!=9999)*(A/1000.0+9999)-9999\""])
            subprocess.call(cmd, shell=True)
            dbio.ingest(dbname, filename, dt, table, False)
            ftp.cwd("../")
        except:
            print("AMSR-E data not available for {0}. Skipping download!".format(dt.strftime("%Y%m%d")))


class Amsre(Soilmoist):

    def __init__(self):
        """Initialize AMSR-E soil moisture object."""
        super(Amsre, self).__init__()
        self.res = 0.25
        self.stddev = 0.01
        self.tablename = "soilmoist.amsre"
