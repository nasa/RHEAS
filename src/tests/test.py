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
import subprocess
from datetime import datetime
import datasets
import dbio
import dssat


def _cultivars(dbname):
    params = [{'p1': 70, 'p2': 0.3, 'p5': 680, 'g2': 590, 'g3': 8.5, 'phint': 50},
              {'p1': 115, 'p2': 0.5, 'p5': 660, 'g2': 450, 'g3': 10.5, 'phint': 65},
              {'p1': 285, 'p2': 0.5, 'p5': 730, 'g2': 620, 'g3': 8.19, 'phint': 38},
              {'p1': 172, 'p2': 0.5, 'p5': 999, 'g2': 398, 'g3': 6.27, 'phint': 75}]
    shapefile = "{0}/tests/basin.shp".format(rpath.data)
    dssat.addCultivar(dbname, shapefile, params)


def _ingestTables(dbname):
    subprocess.call(["{0}/psql".format(rpath.bins), "-d", "rheas", "-f", "{0}/tests/precip_chirps.sql".format(rpath.data)])
    subprocess.call(["{0}/psql".format(rpath.bins), "-d", "rheas", "-f", "{0}/tests/precip_trmm.sql".format(rpath.data)])
    subprocess.call(["{0}/psql".format(rpath.bins), "-d", "rheas", "-f", "{0}/tests/tmax_ncep.sql".format(rpath.data)])
    subprocess.call(["{0}/psql".format(rpath.bins), "-d", "rheas", "-f", "{0}/tests/tmin_ncep.sql".format(rpath.data)])
    subprocess.call(["{0}/psql".format(rpath.bins), "-d", "rheas", "-f", "{0}/tests/wind_ncep.sql".format(rpath.data)])
    subprocess.call(["{0}/psql".format(rpath.bins), "-d", "rheas", "-f", "{0}/tests/precip_chirps_4.sql".format(rpath.data)])
    subprocess.call(["{0}/psql".format(rpath.bins), "-d", "rheas", "-f", "{0}/tests/precip_trmm_4.sql".format(rpath.data)])
    subprocess.call(["{0}/psql".format(rpath.bins), "-d", "rheas", "-f", "{0}/tests/tmax_ncep_4.sql".format(rpath.data)])
    subprocess.call(["{0}/psql".format(rpath.bins), "-d", "rheas", "-f", "{0}/tests/tmin_ncep_4.sql".format(rpath.data)])
    subprocess.call(["{0}/psql".format(rpath.bins), "-d", "rheas", "-f", "{0}/tests/wind_ncep_4.sql".format(rpath.data)])
    db = dbio.connect(dbname)
    cur = db.cursor()
    sql = """create or replace function resampled(_s text, _t text, out result double precision) as
    $func$
    begin
    execute format('select st_scalex(rast) from %s.%s limit 1',quote_ident(_s),quote_ident(_t)) into result;
    end
    $func$ language plpgsql;"""
    cur.execute(sql)
    cur.execute("create or replace view raster_resampled as (select r_table_schema as sname,r_table_name as tname,resampled(r_table_schema,r_table_name) as resolution from raster_columns)")
    cur.execute("create schema soilmoist")
    db.commit()
    subprocess.call(["{0}/psql".format(rpath.bins), "-d", "rheas", "-f", "{0}/tests/soilmoist_smos.sql".format(rpath.data)])


class testDatasets(unittest.TestCase):

    def setUp(self):
        self.dbname = "rheas"
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

    def tearDown(self):
        db = dbio.connect(self.dbname)
        cur = db.cursor()
        for table in ["precip.chirps", "precip.trmm", "tmax.ncep", "tmin.ncep", "wind.ncep"]:
            cur.execute("drop table {0}".format(table))
            cur.execute("drop table {0}_4".format(table))
        cur.execute("drop schema soilmoist cascade")
        db.commit()
        cur.close()
        db.close()


class testNowcast(unittest.TestCase):

    def setUp(self):
        self.dbname = "rheas"
        configfile = "{0}/tests/nowcast.conf".format(rpath.data)
        self.options = config.loadFromFile(configfile)
        self.options['nowcast'][
            'basin'] = "{0}/tests/basin.shp".format(rpath.data)
        self.options['dssat'][
            'shapefile'] = "{0}/tests/basin.shp".format(rpath.data)
        _cultivars(self.dbname)

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
        self.options['nowcast']['startdate'] = "2011-2-1"
        self.options['nowcast']['enddate'] = "2011-2-2"
        nowcast.execute(self.dbname, self.options)

    def tearDown(self):
        db = dbio.connect(self.dbname)
        cur = db.cursor()
        cur.execute("drop table dssat.cultivars")
        db.commit()
        cur.close()
        db.close()


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
