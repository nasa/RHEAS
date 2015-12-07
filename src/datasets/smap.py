"""Class definition for the SMAP Soil Mositure data type.

.. module:: smap
   :synopsis: Definition of the SMAP class

.. moduleauthor:: Kostas Andreadis <kandread@jpl.nasa.gov>

"""

from smos import Smos
import h5py
import tempfile
from ftplib import FTP
import numpy as np
import dbio
import datasets


table = "soilmoist.smap"


def dates(dbname):
    dts = datasets.dates(dbname, table)
    return dts


def download(dbname, dt, bbox=None):
    """Downloads SMAP soil mositure data for a set of dates *dt*
    and imports them into the PostGIS database *dbname*. Optionally
    uses a bounding box to limit the region with [minlon, minlat, maxlon, maxlat]."""
    res = 0.36
    url = "ftp://n5eil01u.ecs.nsidc.org"
    ftp = FTP(url)
    ftp.login()
    ftp.cwd("SAN/SMAP/SPL3SMP.002")
    days = ftp.nlst()
    datadir = dt.strftime("%Y.%m.%d")
    if datadir in days:
        outpath = tempfile.mkdtemp()
        ftp.cwd(datadir)
        fname = [f for f in ftp.nlst() if f.find("h5") > 0][0]
        with open("{0}/{1}".format(outpath, fname), 'wb') as f:
            ftp.retrbinary("RETR {0}".format(fname), f.write)
        f = h5py.File("{0}/{1}".format(outpath, fname))
        lat = f['Soil_Moisture_Retrieval_Data']['latitude'][:, 0]
        lon = f['Soil_Moisture_Retrieval_Data']['longitude'][0, :]
        if bbox is not None:
            i = np.where(np.logical_and(lat > bbox[1], lat < bbox[3]))[0]
            j = np.where(np.logical_and(lon > bbox[0], lon < bbox[2]))[0]
            lat = lat[i]
            lon = lon[j]
        else:
            i = range(len(lat))
            j = range(len(lon))
        sm = f['Soil_Moisture_Retrieval_Data'][
            'soil_moisture'][i[0]:i[-1] + 1, j[0]:j[-1] + 1]
        # FIXME: Use spatially variable observation error
        # sme = f['Soil_Moisture_Retrieval_Data']['soil_moisture_error'][i[0]:i[-1]+1, j[0]:j[-1]+1]
        filename = dbio.writeGeotif(lat, lon, res, sm)
        dbio.ingest(dbname, filename, dt, table, False)


class Smap(Smos):

    def __init__(self):
        """Initialize SMAP soil moisture object."""
        super(Smap, self).__init__()
        self.res = 0.36
        self.stddev = 0.001
        self.tablename = "soilmoist.smap"
