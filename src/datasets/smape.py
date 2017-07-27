"""Class definition for the SMAP Enhanced Soil Mositure data type.

.. module:: smape
   :synopsis: Definition of the SMAPE class

.. moduleauthor:: Kostas Andreadis <kandread@jpl.nasa.gov>

"""

from soilmoist import Soilmoist
from datasets import smap


table = "soilmoist.smape"


dates = smap.dates


def download(dbname, dts, bbox=None):
    smap.table = table
    return smap.download(dbname, dts, bbox, True)


class Smape(Soilmoist):

    def __init__(self, uncert=None):
        """Initialize SMAPE soil moisture object."""
        super(Smape, self).__init__(uncert)
        self.res = 0.09
        self.stddev = 0.001
        self.tablename = "soilmoist.smape"
