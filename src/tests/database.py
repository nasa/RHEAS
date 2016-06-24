""" RHEAS database functionality for testing suite.

   :synopsis: Database functions for the RHEAS testing suite

.. moduleauthor:: Kostas Andreadis <kandread@jpl.nasa.gov>

"""

import subprocess
import rpath
import dssat
import dbio


def cultivars(dbname):
    params = [{'p1': 70, 'p2': 0.3, 'p5': 680, 'g2': 590, 'g3': 8.5, 'phint': 50},
              {'p1': 115, 'p2': 0.5, 'p5': 660, 'g2': 450, 'g3': 10.5, 'phint': 65},
              {'p1': 285, 'p2': 0.5, 'p5': 730, 'g2': 620, 'g3': 8.19, 'phint': 38},
              {'p1': 172, 'p2': 0.5, 'p5': 999, 'g2': 398, 'g3': 6.27, 'phint': 75}]
    shapefile = "{0}/tests/basin.shp".format(rpath.data)
    dssat.addCultivar(dbname, shapefile, params)


def createDatabase(dbname):
    subprocess.call(["{0}/createdb".format(rpath.bins), dbname])
    db = dbio.connect(dbname)
    cur = db.cursor()
    cur.execute("create extension postgis; create extension postgis_topology;")
    cur.execute("create schema vic; create schema dssat; create schema crops;")
    db.commit()
    cur.execute("create table vic.input (resolution double precision,snowbandfile text,vegparam text,veglib text,soilfile text,rootzones integer,basefile text)")
    db.commit()
    cur.execute("insert into vic.input values (0.25, 'vic/global_snowbands_0.25deg.txt', 'vic/global_lai_0.25deg.txt', 'vic/vic_veglib.txt', 'vic/global_soil_0.25deg.txt', 2, 'vic/dssat.inp.base')")
    cur.execute("create schema precip; create schema tmax; create schema tmin; create schema wind; create schema lai")
    cur.execute("create table dssat.cultivars (gid serial primary key, ensemble int, geom geometry, p1 numeric, p2 numeric, p5 numeric, g2 numeric, g3 numeric, phint numeric)")
    db.commit()
    subprocess.call(["{0}/psql".format(rpath.bins), "-d", dbname, "-f", "{0}/tests/vic_soils.sql".format(rpath.data)])
    cur.close()
    db.close()


def dropDatabase(dbname):
    subprocess.call(["{0}/dropdb".format(rpath.bins), dbname])


def ingestTables(dbname):
    subprocess.call(["{0}/psql".format(rpath.bins), "-d", dbname, "-f", "{0}/tests/precip_chirps.sql".format(rpath.data)])
    subprocess.call(["{0}/psql".format(rpath.bins), "-d", dbname, "-f", "{0}/tests/precip_trmm.sql".format(rpath.data)])
    subprocess.call(["{0}/psql".format(rpath.bins), "-d", dbname, "-f", "{0}/tests/tmax_ncep.sql".format(rpath.data)])
    subprocess.call(["{0}/psql".format(rpath.bins), "-d", dbname, "-f", "{0}/tests/tmin_ncep.sql".format(rpath.data)])
    subprocess.call(["{0}/psql".format(rpath.bins), "-d", dbname, "-f", "{0}/tests/wind_ncep.sql".format(rpath.data)])
    subprocess.call(["{0}/psql".format(rpath.bins), "-d", dbname, "-f", "{0}/tests/precip_chirps_4.sql".format(rpath.data)])
    subprocess.call(["{0}/psql".format(rpath.bins), "-d", dbname, "-f", "{0}/tests/precip_trmm_4.sql".format(rpath.data)])
    subprocess.call(["{0}/psql".format(rpath.bins), "-d", dbname, "-f", "{0}/tests/tmax_ncep_4.sql".format(rpath.data)])
    subprocess.call(["{0}/psql".format(rpath.bins), "-d", dbname, "-f", "{0}/tests/tmin_ncep_4.sql".format(rpath.data)])
    subprocess.call(["{0}/psql".format(rpath.bins), "-d", dbname, "-f", "{0}/tests/wind_ncep_4.sql".format(rpath.data)])
    subprocess.call(["{0}/psql".format(rpath.bins), "-d", dbname, "-f", "{0}/tests/cropland.sql".format(rpath.data)])
    subprocess.call(["{0}/psql".format(rpath.bins), "-d", dbname, "-f", "{0}/tests/plantstart.sql".format(rpath.data)])
    subprocess.call(["{0}/psql".format(rpath.bins), "-d", dbname, "-f", "{0}/tests/dssat_soils.sql".format(rpath.data)])
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
    subprocess.call(["{0}/psql".format(rpath.bins), "-d", dbname, "-f", "{0}/tests/soilmoist_smos.sql".format(rpath.data)])
