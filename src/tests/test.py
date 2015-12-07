""" RHEAS nowcast testing suite.

   :synopsis: Unit tests for RHEAS nowcast module

.. moduleauthor:: Kostas Andreadis <kandread@jpl.nasa.gov>

"""

import unittest
import nowcast
import forecast
import config
import rpath
import tempfile


class testDatabase(unittest.TestCase):

    # FIXME: Add unit tests for database
    pass


class testNowcast(unittest.TestCase):

    def setUp(self):
        self.dbname = "rheas"
        configfile = "{0}/tests/nowcast.conf".format(rpath.data)
        self.options = config.loadFromFile(configfile)
        self.options['nowcast'][
            'basin'] = "{0}/tests/basin.shp".format(rpath.data)
        self.options['dssat'][
            'shapefile'] = "{0}/tests/basin.shp".format(rpath.data)

    def testDeterministicVIC(self):
        self.options['nowcast']['model'] = "vic"
        nowcast.execute(self.dbname, self.options)

    def testDeterministicDSSAT(self):
        self.options['nowcast']['model'] = 'vic, dssat'
        nowcast.execute(self.dbname, self.options)

    def testEnsembleVIC(self):
        self.options['vic']['ensemble size'] = 2
        nowcast.execute(self.dbname, self.options)

    def testEnsembleDSSAT(self):
        self.options['nowcast']['model'] = 'vic, dssat'
        self.options['vic']['ensemble size'] = 2
        nowcast.execute(self.dbname, self.options)

    def testAssimilationVIC(self):
        self.options['nowcast']['startdate'] = "2010-1-16"
        self.options['nowcast']['enddate'] = "2010-1-18"
        self.options['vic']['ensemble size'] = 3
        self.options['vic']['observations'] = "smos"
        nowcast.execute(self.dbname, self.options)

    def testMultiplePrecipVIC(self):
        self.options['vic']['precip'] = 'chirps, trmm'
        nowcast.execute(self.dbname, self.options)

    def testVICWithState(self):
        """Test saving and restarting VIC with state file."""
        statepath = tempfile.mkdtemp()
        self.options['vic']['save state'] = statepath
        nowcast.execute(self.dbname, self.options)
        self.options['vic'].pop('save state')
        self.options['vic']['initialize'] = True
        self.options['nowcast']['startdate'] = "2005-12-31"
        self.options['nowcast']['enddate'] = "2006-12-31"
        nowcast.execute(self.dbname, self.options)


class testForecast(unittest.TestCase):

    def setUp(self):
        self.dbname = "rheas"
        configfile = "{0}/tests/forecast.conf".format(rpath.data)
        self.options = config.loadFromFile(configfile)
        self.options['forecast'][
            'basin'] = "{0}/tests/basin.shp".format(rpath.data)
        self.options['dssat'][
            'shapefile'] = "{0}/tests/basin.shp".format(rpath.data)

    def testEspVIC(self):
        self.options['forecast']['ensemble size'] = 2
        self.options['forecast']['method'] = "esp"
        forecast.execute(self.dbname, self.options)

    def testEspVICwithPerturb(self):
        self.options['forecast']['ensemble size'] = 2
        self.options['forecast']['method'] = "esp"
        self.options['vic']['initialize'] = "perturb"
        forecast.execute(self.dbname, self.options)

    def testIriVIC(self):
        self.options['forecast']['startdate'] = "2001-4-1"
        self.options['forecast']['enddate'] = "2001-6-30"
        self.options['forecast']['ensemble size'] = 2
        self.options['forecast']['method'] = "iri"
        forecast.execute(self.dbname, self.options)

    def testEspVICwithAssimilation(self):
        self.options['forecast']['startdate'] = "2010-2-1"
        self.options['forecast']['enddate'] = "2010-4-30"
        self.options['vic']['ensemble size'] = 3
        self.options['forecast']['ensemble size'] = 3
        self.options['vic']['observations'] = "smos"
        forecast.execute(self.dbname, self.options)

    def testEspDSSAT(self):
        self.options['forecast']['model'] = 'vic, dssat'
        self.options['forecast']['ensemble size'] = 2
        self.options['forecast']['method'] = "esp"
        forecast.execute(self.dbname, self.options)


# if __name__ == '__main__':
#     unittest.main()
