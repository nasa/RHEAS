""" RHEAS database functionality for testing suite.

   :synopsis: Database functions for the RHEAS testing suite

.. moduleauthor:: Kostas Andreadis <kandread@jpl.nasa.gov>

"""

import subprocess
import pandas as pd
import rpath
from dssat import utils as dutils
import dbio


def cultivars(dbname):
    """Set cultivar parameters for DSSAT unit tests."""
    params = [{'p1': 70, 'p2': 0.3, 'p5': 680, 'g2': 590, 'g3': 8.5, 'phint': 50},
              {'p1': 115, 'p2': 0.5, 'p5': 660, 'g2': 450, 'g3': 10.5, 'phint': 65},
              {'p1': 285, 'p2': 0.5, 'p5': 730, 'g2': 620, 'g3': 8.19, 'phint': 38},
              {'p1': 172, 'p2': 0.5, 'p5': 999, 'g2': 398, 'g3': 6.27, 'phint': 75}]
    shapefile = "{0}/tests/basin.shp".format(rpath.data)
    dutils.addCultivar(dbname, shapefile, params)


def createDatabase(dbname):
    """Create temporary database for unit testing."""
    subprocess.call(["{0}/createdb".format(rpath.bins), dbname])
    subprocess.call(["{0}/pg_restore".format(rpath.bins), "-d", dbname, "{0}/tests/testdb.dump".format(rpath.data)])
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


def dropDatabase(dbname):
    """Delete temporary database created for unit testing."""
    subprocess.call(["{0}/dropdb".format(rpath.bins), dbname])


def ingestTables(dbname):
    """Ingest datasets needed for the unit tests."""
    for dt in pd.date_range("2011-1-1", "2011-12-31"):
        dbio.ingest(dbname, "{0}/tests/precip/chirps_{1}.tif".format(rpath.data, dt.strftime("%Y-%m-%d")), dt, "precip.chirps")
        dbio.ingest(dbname, "{0}/tests/precip/trmm_{1}.tif".format(rpath.data, dt.strftime("%Y-%m-%d")), dt, "precip.trmm")
        dbio.ingest(dbname, "{0}/tests/tmax/tmax_{1}.tif".format(rpath.data, dt.strftime("%Y-%m-%d")), dt, "tmax.ncep")
        dbio.ingest(dbname, "{0}/tests/tmin/tmin_{1}.tif".format(rpath.data, dt.strftime("%Y-%m-%d")), dt, "tmin.ncep")
        dbio.ingest(dbname, "{0}/tests/wind/wind_{1}.tif".format(rpath.data, dt.strftime("%Y-%m-%d")), dt, "wind.ncep")
        dbio.ingest(dbname, "{0}/tests/soilmoist/smos_{1}.tif".format(rpath.data, dt.strftime("%Y-%m-%d")), dt, "soilmoist.smos")
    subprocess.call(["{0}/psql".format(rpath.bins), "-d", dbname, "-f", "{0}/tests/cropland.sql".format(rpath.data)])
    subprocess.call(["{0}/psql".format(rpath.bins), "-d", dbname, "-f", "{0}/tests/plantstart.sql".format(rpath.data)])
    subprocess.call(["{0}/psql".format(rpath.bins), "-d", dbname, "-f", "{0}/tests/dssat_soils.sql".format(rpath.data)])
