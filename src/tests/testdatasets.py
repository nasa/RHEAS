""" RHEAS dataset testing suite.

   :synopsis: Unit tests for RHEAS dataset download

.. moduleauthor:: Kostas Andreadis <kandread@jpl.nasa.gov>

"""

import unittest
import rpath
import dbio
import datasets
import tests.database
from datetime import datetime


class testDatasets(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        dbname = "testdb"
        tests.database.createDatabase(dbname)

    def setUp(self):
        self.dbname = "testdb"
        configfile = "{0}/tests/data.conf".format(rpath.data)
        conf = datasets.readDatasetList(configfile)
        bbox = map(lambda s: conf.getfloat('domain', s), [
                   'minlon', 'minlat', 'maxlon', 'maxlat'])
        for name in ["chirps", "ncep", "smos", "trmm"]:
            mod = __import__("datasets.{0}".format(name), fromlist=[name])
            t0 = datetime.strptime(conf.get(name, 'startdate'), "%Y-%m-%d")
            t1 = datetime.strptime(conf.get(name, 'enddate'), "%Y-%m-%d")
            mod.download(self.dbname, (t0, t1), bbox)

    def testTable(self):
        db = dbio.connect(self.dbname)
        cur = db.cursor()
        cur.execute("select * from information_schema.tables where table_name='chirps' and table_schema='precip'")
        assert bool(cur.rowcount) is True

    @classmethod
    def tearDownClass(cls):
        dbname = "testdb"
        tests.database.dropDatabase(dbname)
