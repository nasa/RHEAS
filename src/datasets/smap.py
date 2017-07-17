"""Class definition for the SMAP Soil Mositure data type.

.. module:: smap
   :synopsis: Definition of the SMAP class

.. moduleauthor:: Kostas Andreadis <kandread@jpl.nasa.gov>

"""

from soilmoist import Soilmoist
import h5py
import numpy as np
import dbio
import datasets
from datetime import timedelta
import os
import rpath
import logging
import earthdata


table = "soilmoist.smap"


def dates(dbname):
    dts = datasets.dates(dbname, table)
    return dts


def download(dbname, dts, bbox=None):
    """Downloads SMAP soil mositure data for a set of dates *dt*
    and imports them into the PostGIS database *dbname*. Optionally
    uses a bounding box to limit the region with [minlon, minlat, maxlon, maxlat]."""
    log = logging.getLogger(__name__)
    res = 0.36
    url = "https://n5eil01u.ecs.nsidc.org/DP4/SMAP/SPL3SMP.004"
    for dt in [dts[0] + timedelta(tt) for tt in range((dts[-1] - dts[0]).days + 1)]:
        try:
            outpath, fname = earthdata.download("{0}/{1}".format(url, dt.strftime("%Y.%m.%d")), "SMAP_L3_SM_P_\S*.h5")
            f = h5py.File("{0}/{1}".format(outpath, fname))
            lat = f['Soil_Moisture_Retrieval_Data']['latitude'][:, 0]
            lon = f['Soil_Moisture_Retrieval_Data']['longitude'][0, :]
            lon[lon > 180] -= 360.0
            # FIXME: Need to add reprojection from EASE grid
            i1, i2, j1, j2 = datasets.spatialSubset(np.sort(lat)[::-1], np.sort(lon), res, bbox)
            lati = np.argsort(lat)[::-1][i1:i2]
            loni = np.argsort(lon)[j1:j2]
            sm = np.zeros((len(lati), len(loni)))
            for i in range(len(lati)):
                for j in range(len(loni)):
                    sm[i, j] = f['Soil_Moisture_Retrieval_Data']['soil_moisture'][i, j]
            # FIXME: Use spatially variable observation error
            # sme = f['Soil_Moisture_Retrieval_Data']['soil_moisture_error'][i1:i2, j1:j2]
            lat = np.sort(lat)[::-1][i1:i2]
            lon = np.sort(lon)[j1:j2]
            if not os.path.isdir("{0}/soilmoist/smap".format(rpath.data)):
                os.makedirs("{0}/soilmoist/smap".format(rpath.data))
            filename = "{0}/soilmoist/smap/smap_{1}.tif".format(rpath.data, dt.strftime("%Y%m%d"))
            dbio.writeGeotif(lat, lon, res, sm, filename)
            dbio.ingest(dbname, filename, dt, table, False)
        except:
            log.warning("No SMAP data available for {0}.".format(dt.strftime("%Y-%m-%d")))


class Smap(Soilmoist):

    def __init__(self, uncert=None):
        """Initialize SMAP soil moisture object."""
        super(Smap, self).__init__(uncert)
        self.res = 0.36
        self.stddev = 0.001
        self.tablename = "soilmoist.smap"
