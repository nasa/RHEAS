""" RHEAS forecast testing suite.

   :synopsis: Unit tests for RHEAS forecast module

.. moduleauthor:: Kostas Andreadis <kandread@jpl.nasa.gov>

"""

import unittest
import forecast
import rpath
import subprocess
import config
import dbio
import tests.database


class testForecast(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Create dummy database for testing."""
        dbname = "testdb"
        tests.database.createDatabase(dbname)
        tests.database.ingestTables(dbname)
        subprocess.call(["{0}/psql".format(rpath.bins), "-d", dbname, "-f", "{0}/tests/precip_iri.sql".format(rpath.data)])
        subprocess.call(["{0}/psql".format(rpath.bins), "-d", dbname, "-f", "{0}/tests/tmax_iri.sql".format(rpath.data)])

    @classmethod
    def tearDownClass(cls):
        """Delete testing database."""
        dbname = "testdb"
        tests.database.dropDatabase(dbname)

    def setUp(self):
        """Set parameters for forecast unit tests."""
        self.dbname = "testdb"
        configfile = "{0}/tests/forecast.conf".format(rpath.data)
        self.options = config.loadFromFile(configfile)
        self.options['forecast'][
            'basin'] = "{0}/tests/basin.shp".format(rpath.data)
        self.options['dssat'][
            'shapefile'] = "{0}/tests/basin.shp".format(rpath.data)
        tests.database.cultivars(self.dbname)

    def tearDown(self):
        """Clean up data generated after each unit test."""
        db = dbio.connect(self.dbname)
        cur = db.cursor()
        cur.execute("drop schema {0} cascade".format(self.options['forecast']['name']))
        db.commit()
        cur.close()
        db.close()

    def testEspVIC(self):
        """Test ESP forecast VIC simulation, with random initialization."""
        self.options['forecast']['ensemble size'] = 2
        self.options['forecast']['method'] = "esp"
        forecast.execute(self.dbname, self.options)

    def testEspVICwithPerturb(self):
        """Test ESP forecast VIC simulation, with initialization
        from perturbed model simulations."""
        self.options['forecast']['ensemble size'] = 2
        self.options['forecast']['method'] = "esp"
        self.options['vic']['initialize'] = "perturb"
        forecast.execute(self.dbname, self.options)

    def testIriVIC(self):
        """Test forecast VIC simulations using IRI forecast data."""
        self.options['forecast']['startdate'] = "2012-2-1"
        self.options['forecast']['enddate'] = "2012-4-30"
        self.options['forecast']['ensemble size'] = 1
        self.options['forecast']['method'] = "iri"
        forecast.execute(self.dbname, self.options)

    def testEspVICwithAssimilation(self):
        """Test ESP forecast VIC simulation with data assimilation."""
        self.options['forecast']['startdate'] = "2011-1-1"
        self.options['forecast']['enddate'] = "2011-1-31"
        self.options['vic']['ensemble size'] = 3
        self.options['forecast']['ensemble size'] = 3
        self.options['vic']['observations'] = "smos"
        forecast.execute(self.dbname, self.options)

    def testEspDSSAT(self):
        """Test ESP forecast DSSAT simulation."""
        self.options['forecast']['model'] = 'vic, dssat'
        self.options['forecast']['ensemble size'] = 2
        self.options['forecast']['method'] = "esp"
        self.options['forecast']['startdate'] = "2011-2-1"
        self.options['forecast']['enddate'] = "2011-4-30"
        forecast.execute(self.dbname, self.options)
