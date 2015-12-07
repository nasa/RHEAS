"""Class definition for the AMSR-E Soil Mositure data type.

.. module:: amsre
   :synopsis: Definition of the AMSRE class

.. moduleauthor:: Kostas Andreadis <kandread@jpl.nasa.gov>

"""

from smos import Smos
import datasets


def dates(dbname):
    dts = datasets.dates(dbname, "soilmoist.amsre")
    return dts


def download(dbname, dts, bbox):
    pass


class Amsre(Smos):

    def __init__(self):
        """Initialize AMSR-E soil moisture object."""
        super(Amsre, self).__init__()
        self.res = 0.25
        self.stddev = 0.01
        self.tablename = "soilmoist.amsre"
