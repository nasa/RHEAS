""" RHEAS nowcast testing suite.

   :synopsis: Unit tests for RHEAS nowcast module

.. moduleauthor:: Kostas Andreadis <kandread@jpl.nasa.gov>

"""

import unittest
import nowcast
import rpath
import config
import dbio
import tempfile
import tests.database


class testNowcast(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Create dummy database for testing."""
        dbname = "testdb"
        tests.database.createDatabase(dbname)

    @classmethod
    def tearDownClass(cls):
        """Delete testing database."""
        dbname = "testdb"
        tests.database.dropDatabase(dbname)

    def setUp(self):
        """Set parameters for nowcast unit tests."""
        self.dbname = "testdb"
        configfile = "{0}/tests/nowcast.conf".format(rpath.data)
        self.options = config.loadFromFile(configfile)
        self.options['nowcast'][
            'basin'] = "{0}/tests/basin.shp".format(rpath.data)
        self.options['dssat'][
            'shapefile'] = "{0}/tests/basin.shp".format(rpath.data)
        self.options['nowcast']['model'] = "vic"
        tests.database.cultivars(self.dbname)

    def tearDown(self):
        """Clean up data generated after each unit test."""
        db = dbio.connect(self.dbname)
        cur = db.cursor()
        cur.execute("drop schema {0} cascade".format(self.options['nowcast']['name']))
        db.commit()
        cur.close()
        db.close()

    def testDeterministicVIC(self):
        """Test deterministic nowcast VIC simulation."""
        nowcast.execute(self.dbname, self.options)
        db = dbio.connect(self.dbname)
        cur = db.cursor()
        cur.execute("select * from information_schema.tables where table_name='runoff' and table_schema='basin'")
        assert bool(cur.rowcount) is True
        cur.close()
        db.close()

    def testDeterministicDSSAT(self):
        """Test deterministic nowcast DSSAT simulation."""
        self.options['nowcast']['model'] = 'vic, dssat'
        self.options['nowcast']['startdate'] = "2011-1-21"
        self.options['nowcast']['enddate'] = "2011-4-30"
        nowcast.execute(self.dbname, self.options)

    def testEnsembleVIC(self):
        """Test ensemble nowcast VIC simulation."""
        self.options['vic']['ensemble size'] = 2
        nowcast.execute(self.dbname, self.options)

    def testEnsembleDSSAT(self):
        """Test ensemble nowcast DSSAT simulation."""
        self.options['nowcast']['model'] = 'vic, dssat'
        self.options['vic']['ensemble size'] = 2
        self.options['nowcast']['startdate'] = "2011-2-21"
        self.options['nowcast']['enddate'] = "2011-4-30"
        nowcast.execute(self.dbname, self.options)

    def testAssimilationVIC(self):
        """Test nowcast VIC simulation with data assimilation."""
        self.options['nowcast']['startdate'] = "2011-1-1"
        self.options['nowcast']['enddate'] = "2011-1-2"
        self.options['vic']['ensemble size'] = 3
        self.options['vic']['observations'] = "smos"
        nowcast.execute(self.dbname, self.options)

    def testMultiplePrecipVIC(self):
        """Test VIC simulation with multiple precipitation datasets."""
        self.options['vic']['precip'] = 'chirps, trmm'
        nowcast.execute(self.dbname, self.options)

    def testVICWithState(self):
        """Test saving and restarting VIC with state file."""
        statepath = tempfile.mkdtemp()
        self.options['vic']['save state'] = statepath
        nowcast.execute(self.dbname, self.options)
        self.options['vic'].pop('save state')
        self.options['vic']['initialize'] = True
        self.options['nowcast']['startdate'] = "2011-2-1"
        self.options['nowcast']['enddate'] = "2011-2-2"
        nowcast.execute(self.dbname, self.options)
