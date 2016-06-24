""" RHEAS module for retrieving water storage anomaly data from the
    GRACE satellite mission.

.. module:: grace
   :synopsis: Retrieve GRACE TWS data

.. moduleauthor:: Kostas Andreadis <kandread@jpl.nasa.gov>

"""

from datasets.decorators import geotiff, http
from datetime import timedelta
import datasets


table = "tws.grace"


@geotiff
@http
def fetch(dbname, dt, bbox):
    """Downloads GRACE water storage anomaly data from the data server."""
    url = "ftp://podaac-ftp.jpl.nasa.gov/allData/tellus/L3/land_mass//RL05/geotiff/GRCTellus.CSR.{0:04d}{1:02d}{2:02d}.LND.RL05.DSTvSCS1409.tif.gz"
    return url, bbox, dt


@geotiff
@http
def fetchScalingGrid(dbname, dt, bbox):
    """Downloads GRACE scaling factor grid, in order to multiply the retrieved
    land data. See http://grace.jpl.nasa.gov/data/get-data/monthly-mass-grids-land for details."""
    url = "ftp://podaac-ftp.jpl.nasa.gov/allData/tellus/L3/land_mass//RL05/geotiff/CLM4.SCALE_FACTOR.DS.G300KM.RL05.DSTvSCS1409.tif.gz"
    return url, bbox, dt


def download(dbname, dts, bbox=None):
    res = 1.0
    sdata, _, _, _ = fetchScalingGrid(dbname, dts[0], bbox)
    for dt in [dts[0] + timedelta(tt) for tt in range((dts[-1] - dts[0]).days + 1)]:
        try:
            data, lat, lon, t = fetch(dbname, dt, bbox)
            data *= sdata
            datasets.ingest(dbname, table, data, lat, lon, res, t, False)
        except:
            pass


def dates(dbname):
    dts = datasets.dates(dbname, table)
    return dts


class Grace:

    pass
