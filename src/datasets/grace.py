""" RHEAS module for retrieving water storage anomaly data from the
    GRACE satellite mission.

.. module:: grace
   :synopsis: Retrieve GRACE TWS data

.. moduleauthor:: Kostas Andreadis <kandread@jpl.nasa.gov>

"""

from datasets.decorators import geotiff
import datasets


table = "tws.grace"


@geotiff
def fetch(dbname, dt, bbox):
    """Downloads GRACE water storage anomaly data from the data server."""
    url = "ftp://podaac-ftp.jpl.nasa.gov/allData/tellus/L3/land_mass//RL05/geotiff/GRCTellus.CSR.{0:04d}{1:02d}{2:02d}.LND.RL05.DSTvSCS1409.tif.gz"
    return url, bbox, dt


@geotiff
def fetchScalingGrid(dbname, dt, bbox):
    """Downloads GRACE scaling factor grid, in order to multiply the retrieved
    land data. See http://grace.jpl.nasa.gov/data/get-data/monthly-mass-grids-land for details."""
    url = "ftp://podaac-ftp.jpl.nasa.gov/allData/tellus/L3/land_mass//RL05/geotiff/CLM4.SCALE_FACTOR.DS.G300KM.RL05.DSTvSCS1409.tif.gz"
    return url, bbox, dt


def download(dbname, dt, bbox=None):
    res = 1.0
    data, lat, lon, t = fetch(dbname, dt, bbox)
    sdata, _, _, _ = fetchScalingGrid(dbname, dt, bbox)
    data *= sdata
    datasets.ingest(dbname, table, data, lat, lon, res, t)


def dates(dbname):
    dts = datasets.dates(dbname, table)
    return dts


class Grace:

    pass
