""" Class definition for the AMSR-E Soil Mositure data type.

.. module:: amsre
   :synopsis: Definition of the AMSRE class

.. moduleauthor:: Kostas Andreadis <kandread@jpl.nasa.gov>

"""

from soilmoist import Soilmoist
from datetime import timedelta
import subprocess
import datasets
import dbio
import earthdata
import logging


table = "soilmoist.amsre"


def dates(dbname):
    dts = datasets.dates(dbname, "soilmoist.amsre")
    return dts


def download(dbname, dts, bbox):
    """Downloads AMSR-E soil moisture data for a set of dates *dts*
    and imports them into the PostGIS database *outpath*. Optionally
    uses a bounding box to limit the region with [minlon, minlat, maxlon, maxlat]."""
    log = logging.getLogger(__name__)
    url = "https://n5eil01u.ecs.nsidc.org/AMSA/AE_Land3.002"
    for dt in [dts[0] + timedelta(ti) for ti in range((dts[-1] - dts[0]).days+1)]:
        try:
            tmppath, fname = earthdata.download("{0}/{1}".format(url, dt.strftime("%Y.%m.%d")), "AMSR_E_L3_DailyLand\S*.hdf")
            proc = subprocess.Popen(["gdal_translate", "HDF4_EOS:EOS_GRID:{0}/{1}:Ascending_Land_Grid:A_Soil_Moisture".format(tmppath, fname), "{0}/sma.tif".format(tmppath)], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            out, err = proc.communicate()
            log.debug(out)
            proc = subprocess.Popen(["gdal_translate", "HDF4_EOS:EOS_GRID:{0}/{1}:Descending_Land_Grid:D_Soil_Moisture".format(tmppath, fname), "{0}/smd.tif".format(tmppath)], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            out, err = proc.communicate()
            log.debug(out)
            # merge orbits
            proc = subprocess.Popen(["gdal_merge.py", "-o", "{0}/sm1.tif".format(tmppath), "{0}/sma.tif".format(tmppath), "{0}/smd.tif".format(tmppath)], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            out, err = proc.communicate()
            log.debug(out)
            # reproject data
            proc = subprocess.Popen(["gdalwarp", "-s_srs", "epsg:3410", "-t_srs", "epsg:4326", "{0}/sm1.tif".format(tmppath), "{0}/sm2.tif".format(tmppath)], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            out, err = proc.communicate()
            log.debug(out)
            if bbox is None:
                pstr = []
            else:
                pstr = ["-projwin", str(bbox[0]), str(bbox[3]), str(bbox[2]), str(bbox[1])]
            proc = subprocess.Popen(["gdal_translate"] + pstr + ["-ot", "Float32", "{0}/sm2.tif".format(tmppath), "{0}/sm3.tif".format(tmppath)], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            out, err = proc.communicate()
            log.debug(out)
            filename = "{0}/amsre_soilm_{1}.tif".format(tmppath, dt.strftime("%Y%m%d"))
            proc = subprocess.Popen(["gdal_calc.py", "-A", "{0}/sm3.tif".format(tmppath), "--outfile={0}".format(filename), "--NoDataValue=-9999", "--calc=(abs(A)!=9999)*(A/1000.0+9999)-9999"], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            out, err = proc.communicate()
            log.debug(out)
            dbio.ingest(dbname, filename, dt, table, False)
        except:
            log.warning("AMSR-E data not available for {0}. Skipping download!".format(dt.strftime("%Y%m%d")))


class Amsre(Soilmoist):

    def __init__(self, uncert=None):
        """Initialize AMSR-E soil moisture object."""
        super(Amsre, self).__init__(uncert)
        self.res = 0.25
        self.stddev = 0.01
        self.tablename = "soilmoist.amsre"
