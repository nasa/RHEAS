"""Class definition for the SMAP Soil Mositure data type.

.. module:: smap
   :synopsis: Definition of the SMAP class

.. moduleauthor:: Kostas Andreadis <kandread@jpl.nasa.gov>

"""

from soilmoist import Soilmoist
import h5py
import tempfile
from ftplib import FTP
import numpy as np
import dbio
import datasets
from datetime import timedelta


table = "soilmoist.smap"


def dates(dbname):
    dts = datasets.dates(dbname, table)
    return dts


def download(dbname, dts, bbox=None):
    """Downloads SMAP soil mositure data for a set of dates *dt*
    and imports them into the PostGIS database *dbname*. Optionally
    uses a bounding box to limit the region with [minlon, minlat, maxlon, maxlat]."""
    res = 0.36
    url = "n5eil01u.ecs.nsidc.org"
    ftp = FTP(url)
    ftp.login()
    for dt in [dts[0] + timedelta(tt) for tt in range((dts[-1] - dts[0]).days + 1)]:
        r = ftp.cwd("/pub/SAN/SMAP/SPL3SMP.003/{0}".format(dt.strftime("%Y.%m.%d")))
        if r.find("successful") > 0:
            outpath = tempfile.mkdtemp()
            fname = [f for f in ftp.nlst() if f.find("h5") > 0][0]
            with open("{0}/{1}".format(outpath, fname), 'wb') as f:
                ftp.retrbinary("RETR {0}".format(fname), f.write)
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
            filename = dbio.writeGeotif(lat, lon, res, sm)
            dbio.ingest(dbname, filename, dt, table, False)
        else:
            print("No SMAP data available for {0}.".format(dt.strftime("%Y-%m-%d")))


class Smap(Soilmoist):

    def __init__(self):
        """Initialize SMAP soil moisture object."""
        super(Smap, self).__init__()
        self.res = 0.36
        self.stddev = 0.001
        self.tablename = "soilmoist.smap"
