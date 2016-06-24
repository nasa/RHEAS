""" Class definition for the VIC model interface

.. module:: vic
   :synopsis: Definition of the VIC model class

.. moduleauthor:: Kostas Andreadis <kandread@jpl.nasa.gov>

"""

from __future__ import division
import output as vicoutput
from osgeo import ogr, gdal, osr
import decimal
import sys
import subprocess
import os
import string
import numpy
from datetime import date, datetime, timedelta
import multiprocessing as mp
import shutil
import numpy as np
from collections import OrderedDict
import drought
import pandas
import dbio
import rpath
import random
from raster import TileReader


class VIC:

    def __init__(self, path, dbname, resolution, startyear, startmonth, startday,
                 endyear, endmonth, endday, name="", savestate="", nlayer=3):
        self.model_path = path
        self.nodata = -9999.
        if bool(name):
            self.name = name
        else:
            self.name = None
        self.startyear = startyear
        self.startmonth = startmonth
        self.startday = startday
        self.startdate = datetime(startyear, startmonth, startday)
        self.endyear = endyear
        self.endmonth = endmonth
        self.endday = endday
        self.enddate = datetime(endyear, endmonth, endday)
        self.nlayers = nlayer
        self.dbname = dbname
        db = dbio.connect(dbname)
        cur = db.cursor()
        cur.execute(
            "select resolution from vic.input order by abs(resolution - {0})".format(resolution))
        if not bool(cur.rowcount):
            print("No appropriate VIC input files found in the database. Exiting!")
            sys.exit()
        self.res = cur.fetchone()[0]
        cur.close()
        try:
            self.grid_decimal = - \
                (decimal.Decimal(self.res).as_tuple().exponent - 1)
        except:
            self.grid_decimal = - \
                (decimal.Decimal(str(self.res)).as_tuple().exponent - 1)
        self.lat = []
        self.lon = []
        self.gid = OrderedDict()
        self.lgid = OrderedDict()
        self.depths = OrderedDict()
        self.skipyear = 0
        self.elev = OrderedDict()
        self.statefile = ""

    def paramFromDB(self):
        """Retrieve file parameters from database."""
        db = dbio.connect(self.dbname)
        cur = db.cursor()
        # cur = self.db.cursor()
        cur.execute(
            'select veglib,vegparam,snowbandfile from vic.input where resolution=%f;' % self.res)
        veglib, vegparam, snowbands = cur.fetchone()
        cur.close()
        db.close()
        return veglib, vegparam, snowbands

    def _getSnowbands(self, snowbands):
        """Find number of snow bands from file."""
        filename = "{0}/{1}".format(rpath.data, snowbands)
        with file(filename) as f:
            line = f.readline()
        return int((len(line.split()) - 1) / 3)

    def writeSoilFile(self, shapefile):
        """Write soil parameter file for current simulation based on basin shapefile."""
        ds = ogr.Open(shapefile)
        lyr = ds.GetLayer()
        db = dbio.connect(self.dbname)
        cur = db.cursor()
        cur.execute(
            "select * from information_schema.tables where table_name='basin' and table_schema=%s", (self.name,))
        if not bool(cur.rowcount):
            temptable = ''.join(random.SystemRandom().choice(
                string.ascii_letters) for _ in range(8))
            cur.execute(
                "create table {0}(gid serial primary key, geom geometry)".format(temptable))
            for i in range(lyr.GetFeatureCount()):
                f = lyr.GetNextFeature()
                g = f.GetGeometryRef()
                cur.execute("insert into {0}(geom) values(st_geomfromtext('{1}',4326))".format(
                    temptable, g.ExportToWkt()))
            sql = "select updategeometrysrid('{0}','geom',4326)".format(
                temptable)
            db.commit()
            cur.execute(
                "create index {0}_s on {0} using gist(geom)".format(temptable))
            ds = None
            cur.execute(
                "select schema_name from information_schema.schemata where schema_name='{0}'".format(self.name))
            if not bool(cur.rowcount):
                cur.execute("create schema {0}".format(self.name))
            sql = "create table {0}.basin (gid integer, elev real, depths real[], geom geometry(Point, 4326), line text, constraint {0}_gidkey primary key(gid),  CONSTRAINT enforce_dims_geom CHECK (st_ndims(geom) = 2), CONSTRAINT enforce_geotype_geom CHECK (geometrytype(geom) = 'POINT'::text OR geom IS NULL))".format(self.name)
            cur.execute(sql)
            sql = "insert into {0}.basin (gid, elev, depths, geom, line) select v.id,v.elev,v.depths,v.geom,v.line from vic.soils as v,{1} as t where st_intersects(v.geom,t.geom) and resolution={2}".format(
                self.name, temptable, self.res)
            cur.execute(sql)
            cur.execute("drop table {0}".format(temptable))
            db.commit()
            cur.execute(
                "create index basin_s on {0}.basin using gist(geom)".format(self.name))
            db.commit()
        sql = "select line,gid,st_y(geom),st_x(geom),elev,depths from {0}.basin order by gid".format(
            self.name)
        cur.execute(sql)
        lines = cur.fetchall()
        with open(self.model_path + '/soil.txt', 'w') as fout:
            for line in lines:
                gid, lat, lon, elev, depths = line[1:]
                fout.write("{0}\n".format(line[0]))
                self.lat.append(lat)
                self.lon.append(lon)
                self.gid[gid] = (lat, lon)
                self.lgid[(lat, lon)] = gid
                self.depths[gid] = depths
                self.elev[gid] = elev
        cur.execute("alter table {0}.basin drop column line".format(self.name))
        cur.close()
        db.close()

    def stateFile(self):
        """Retrieve state file path from database."""
        db = dbio.connect(self.dbname)
        cur = db.cursor()
        sql = "select filename from {0}.state where fdate = date '{1}-{2}-{3}'".format(
            self.name, self.startyear, self.startmonth, self.startday)
        cur.execute(sql)
        result = cur.fetchone()
        if bool(result):
            filename = result[0]
        else:
            print(
                "WARNING! No state file found for requested date, initializing from default.")
            filename = None
        cur.close()
        db.close()
        return filename

    def _stateToDb(self, statefilepath):
        """Add path to state file into database."""
        db = dbio.connect(self.dbname)
        cur = db.cursor()
        cur.execute(
            "select schema_name from information_schema.schemata where schema_name='{0}'".format(self.name))
        if not bool(cur.rowcount):
            cur.execute("create schema {0}".format(self.name))
            db.commit()
        cur.execute(
            "select table_name from information_schema.tables where table_schema='{0}' and table_name='state'".format(self.name))
        if not bool(cur.rowcount):
            sql = "create table {0}.state (filename text, fdate date)".format(
                self.name)
            cur.execute(sql)
            db.commit()
        statefile = "{0}/vic.state_{1:04d}{2:02d}{3:02d}".format(
            statefilepath, self.endyear, self.endmonth, self.endday)
        statedate = "{0}-{1}-{2}".format(self.endyear,
                                         self.endmonth, self.endday)
        cur.execute(
            "select * from {0}.state where fdate=date '{1}'".format(self.name, statedate))
        if bool(cur.rowcount):
            sql = "update {0}.state set filename='{1}' where fdate=date '{2}'".format(
                self.name, statefile, statedate)
        else:
            sql = "insert into {0}.state values ('{1}', date '{2}')".format(
                self.name, statefile, statedate)
        cur.execute(sql)
        db.commit()
        cur.close()
        db.close()

    def writeParamFile(self, nodes=3, time_step=24, save_state="", init_state=False, state_file="", save_state_to_db=False):
        """Write VIC global parameter file for current simulation."""
        db = dbio.connect(self.dbname)
        cur = db.cursor()
        cur.execute(
            'select rootzones from vic.input where resolution=%f;' % self.res)
        root_zones = cur.fetchone()[0]
        cur.close()
        db.close()
        # self.nlayers = nlayer
        fout = open(self.model_path + '/global.txt', 'w')
        fout.write("NLAYER\t{0:d}\n".format(self.nlayers))
        fout.write("NODES\t{0:d}\n".format(nodes))
        if time_step < 24:
            fout.write("TIME_STEP\t{0:d}\nSNOW_STEP\t{1:d}\n".format(
                time_step, time_step))
            fout.write("FULL_ENERGY\tTRUE\nFROZEN_SOIL\tTRUE\n")
        else:
            fout.write("TIME_STEP\t24\nSNOW_STEP\t3\n")
            fout.write("FULL_ENERGY\tFALSE\nFROZEN_SOIL\tFALSE\n")
        fout.write("STARTYEAR\t{0:04d}\n".format(self.startyear))
        fout.write("STARTMONTH\t{0:02d}\n".format(self.startmonth))
        fout.write("STARTDAY\t{0:02d}\n".format(self.startday))
        fout.write("ENDYEAR\t{0:04d}\n".format(self.endyear))
        fout.write("ENDMONTH\t{0:02d}\n".format(self.endmonth))
        fout.write("ENDDAY\t{0:02d}\n".format(self.endday))
        fout.write("IMPLICIT\tFALSE\nTFALLBACK\tTRUE\n")
        fout.write("SNOW_ALBEDO\tUSACE\nSNOW_DENSITY\tDENS_SNTHRM\n")
        fout.write("BLOWING\tFALSE\nCOMPUTE_TREELINE\tFALSE\n")
        fout.write("DIST_PRCP\tFALSE\nPREC_EXPT\t0.6\nCORRPREC\tFALSE\n")
        fout.write("MAX_SNOW_TEMP\t0.5\nMIN_RAIN_TEMP\t-0.5\n")
        fout.write("MIN_WIND_SPEED\t0.1\nAERO_RESIST_CANSNOW\tAR_406_FULL\n")
        if bool(state_file):
            statefile = state_file
        elif init_state:
            statefile = self.stateFile()
        else:
            statefile = None
        if statefile:
            fout.write("INIT_STATE\t{0:s}\n".format(statefile))
        if bool(save_state):
            if isinstance(save_state, str):
                if not os.path.isdir(save_state):
                    os.mkdir(save_state)
                fout.write("STATENAME\t{0}/vic.state\n".format(save_state))
            else:
                fout.write(
                    "STATENAME\t{0}/vic.state\n".format(self.model_path))
            fout.write("STATEYEAR\t{0:04d}\n".format(self.endyear))
            fout.write("STATEMONTH\t{0:02d}\n".format(self.endmonth))
            fout.write("STATEDAY\t{0:02d}\n".format(self.endday))
            self.statefile = "vic.state_{0:04d}{1:02d}{2:02d}".format(
                self.endyear, self.endmonth, self.endday)
            if save_state_to_db:
                self._stateToDb(save_state)
        fout.write("BINARY_STATE_FILE\tFALSE\n")
        fout.write(
            "FORCING1\t{0:s}/data_\n".format(self.model_path + "/forcings"))
        fout.write("FORCE_FORMAT\tASCII\nFORCE_ENDIAN\tLITTLE\nN_TYPES\t4\n")
        fout.write("FORCE_TYPE\tPREC\n")
        fout.write("FORCE_TYPE\tTMAX\n")
        fout.write("FORCE_TYPE\tTMIN\n")
        fout.write("FORCE_TYPE\tWIND\n")
        fout.write("FORCE_DT\t24\n")
        fout.write("FORCEYEAR\t{0:04d}\n".format(self.startyear))
        fout.write("FORCEMONTH\t{0:02d}\n".format(self.startmonth))
        fout.write("FORCEDAY\t{0:02d}\n".format(self.startday))
        fout.write("FORCEHOUR\t0\n")
        fout.write("GRID_DECIMAL\t{0:d}\n".format(self.grid_decimal))
        fout.write("WIND_H\t10.0\nMEASURE_H\t2.0\nALMA_INPUT\tFALSE\n")
        fout.write("SOIL\t{0:s}\n".format(self.model_path + "/soil.txt"))
        veglib, vegparam, snowbands = self.paramFromDB()
        fout.write("VEGLIB\t{0}/{1}\n".format(rpath.data, veglib))
        fout.write("VEGPARAM\t{0}/{1}\n".format(rpath.data, vegparam))
        fout.write("VEGPARAM_LAI\tTRUE\n")
        fout.write("ROOT_ZONES\t{0:d}\n".format(root_zones))
        fout.write("LAI_SRC\tLAI_FROM_VEGPARAM\n")
        nbands = self._getSnowbands(snowbands)
        fout.write(
            "SNOW_BAND\t{0:d}\t{1}/{2}\n".format(nbands, rpath.data, snowbands))
        fout.write("RESULT_DIR\t{0}/output\n".format(self.model_path))
        fout.write("OUT_STEP\t24\n")
        fout.write("BINARY_OUTPUT\tFALSE\n")
        fout.write("MOISTFRACT\tFALSE\n")
        fout.write(
            "COMPRESS\tFALSE\nALMA_OUTPUT\tFALSE\nPTR_HEADER\tFALSE\nPRT_SNOW_BAND\tFALSE\n")
        fout.write(vicoutput.template(["eb", "wb", "sub", "sur", "csp"]))
        fout.close()

    def createIndexTable(self, dataset):
        """Creates index table from raster row, column, and tile for each grid cell."""
        db = dbio.connect(self.dbname)
        cur = db.cursor()
        sname, tname = dataset.split(".")
        cur.execute(
            "select * from raster_resampled where sname='{0}' and tname like '{1}_%' and resolution={2}".format(sname, tname, self.res))
        rtable = ".".join(cur.fetchone()[:2])
        cur.execute("select * from information_schema.tables where table_name='{0}_xy' and table_schema='public'".format(sname))
        if bool(cur.rowcount):
            cur.execute("drop table {0}_xy".format(sname))
            db.commit()
        sql = "create table {0}_xy as (select gid,st_worldtorastercoordx(rast,geom) as x,st_worldtorastercoordy(rast,geom) as y,rid as tile from {4},{5}.basin where fdate=date'{1}-{2}-{3}' and st_intersects(rast,geom))".format(
            sname, self.startyear, self.startmonth, self.startday, rtable, self.name)
        cur.execute(sql)
        cur.execute("create index {0}_xy_r on {0}_xy(tile)".format(sname))
        db.commit()
        cur.close()
        db.close()
        return rtable

    def _getTiles(self, itable):
        """Get raster tile IDs for the domain."""
        db = dbio.connect(self.dbname)
        cur = db.cursor()
        cur.execute("select distinct(tile) from {0}".format(itable))
        tiles = [int(r[0]) for r in cur.fetchall()]
        cur.close()
        db.close()
        return tiles

    def _dropIndexTable(self, sname):
        """Deletes index table."""
        db = dbio.connect(self.dbname)
        cur = db.cursor()
        cur.execute("drop table {0}_xy".format(sname))
        db.commit()
        cur.close()
        db.close()

    def _getTileData(self, rtable, t):
        """Retrieve data from *rtable* for specific tile *t*."""
        db = dbio.connect(self.dbname)
        cur = db.cursor()
        var = rtable.split(".")[0]
        sql = "select gid,fdate,st_value(rast,x,y) from {0},{1}_xy where rid=tile and tile={8} and fdate>=date'{2}-{3}-{4}' and fdate<=date'{5}-{6}-{7}' order by gid,fdate".format(
            rtable, var, self.startyear, self.startmonth, self.startday, self.endyear, self.endmonth, self.endday, t)
        cur.execute(sql)
        data = cur.fetchall()
        return data

    def getForcings(self, options):
        """Get meteorological forcings from database."""
        if not ('precip' in options and 'temperature' in options and 'wind' in options):
            print "ERROR! No data source provided for VIC forcings"
            sys.exit()
        datasets = ["precip." + options["precip"], "tmax." + options["temperature"],
                    "tmin." + options["temperature"], "wind." + options["wind"]]
        if 'lai' in options:
            datasets.append("lai" + options["lai"])
            self.lai = options['lai']
        options['tmax'] = options['temperature']
        options['tmin'] = options['temperature']
        rtables = {}
        for v in ['precip', 'tmax', 'tmin', 'wind']:
            rtables[v] = self.createIndexTable("{0}.{1}".format(v, options[v]))
        tiles = {v: self._getTiles("{0}_xy".format(v))
                 for v in ['precip', 'tmax', 'tmin', 'wind']}
        data = {}
        nprocs = mp.cpu_count()
        p = mp.Pool(nprocs)
        for v in tiles:
            reader = TileReader(self.dbname, rtables[
                                v], self.startyear, self.startmonth, self.startday, self.endyear, self.endmonth, self.endday)
            data[v] = p.map_async(reader, tiles[v])
        data = {v: [i for s in data[v].get() for i in s] for v in data}
        p.close()
        p.join()
        for s in data:
            self._dropIndexTable(s)
        self.precip = options['precip']
        self.temp = options['temperature']
        self.wind = options['wind']
        return data['precip'], data['tmax'], data['tmin'], data['wind']

    def writeForcings(self, prec, tmax, tmin, wind, lai=None):
        """Write VIC meteorological forcing data files."""
        if not os.path.exists(self.model_path + '/forcings'):
            os.mkdir(self.model_path + '/forcings')
        ndays = (date(self.endyear, self.endmonth, self.endday) -
                 date(self.startyear, self.startmonth, self.startday)).days + 1
        assert len(prec) == len(self.lat) * ndays and len(tmax) == len(self.lat) * ndays and len(
            tmin) == len(self.lat) * ndays and len(wind) == len(self.lat) * ndays
        cgid = None
        fout = None
        for i in range(len(prec)):
            gid = prec[i][0]
            if cgid is None or gid != cgid:
                if cgid is not None:
                    fout.close()
                cgid = gid
                filename = "data_{0:.{2}f}_{1:.{2}f}".format(
                    self.gid[gid][0], self.gid[gid][1], self.grid_decimal)
                fout = open(
                    "{0}/forcings/{1}".format(self.model_path, filename), 'w')
                print "writing " + filename
                fout.write("{0:f} {1:.2f} {2:.2f} {3:.1f}\n".format(
                    prec[i][2], tmax[i][2], tmin[i][2], wind[i][2]))
            else:
                fout.write("{0:f} {1:.2f} {2:.2f} {3:.1f}\n".format(
                    prec[i][2], tmax[i][2], tmin[i][2], wind[i][2]))

    def run(self, vicexec):
        """Run VIC model."""
        if not os.path.exists(self.model_path + '/output'):
            os.mkdir(self.model_path + '/output')
        subprocess.call(
            "{0} -g {1}/global.txt".format(vicexec, self.model_path), shell=True)

    def getOutputStruct(self, globalfile):
        """Creates a dictionary with output variable-file pairs."""
        fin = open(globalfile)
        prefix = None
        skipyear = 0
        c = 3  # Assumes daily output
        out = {}
        for line in fin:
            if line.find("OUTFILE") == 0:
                prefix = line.split()[1]
                c = 3
            elif line.find("SKIPYEAR") == 0:
                skipyear = int(line.split()[1])
            else:
                if len(line) > 1 and line[0] != "#" and prefix:
                    varname = line.split()[1].replace("OUT_", "").lower()
                    out[varname] = ("output/" + prefix, c)
                    if varname in ["soil_moist", "soil_temp", "smliqfrac", "smfrozfrac"]:
                        c += self.nlayers
                    else:
                        c += 1
        fin.close()
        out['tmax'] = ("forcings/data", 1)
        out['tmin'] = ("forcings/data", 2)
        out['rainf'] = ("forcings/data", 0)
        self.skipyear = skipyear
        return out

    def readOutput(self, args):
        """Reads VIC output for selected variables."""
        droughtvars = ["spi1", "spi3", "spi6", "spi12", "sri1", "sri3", "sri6", "sri12", "severity", "dryspells", "smdi"]
        layervars = ["soil_moist", "soil_temp", "smliqfrac", "smfrozfrac"]
        outvars = self.getOutputStruct(self.model_path + "/global.txt")
        outdata = {}
        if len(self.lat) > 0 and len(self.lon) > 0:
            nrows = int(np.round((max(self.lat) - min(self.lat)) / self.res) + 1)
            ncols = int(np.round((max(self.lon) - min(self.lon)) / self.res) + 1)
            nt = (date(self.endyear, self.endmonth, self.endday) -
                  date(self.startyear + self.skipyear, self.startmonth, self.startday)).days + 1
            args = vicoutput.variableGroup(args)
            if len(args) > 0:
                for var in args:
                    if var in outvars or var in droughtvars:
                        if var in layervars:
                            outdata[var] = numpy.zeros(
                                (nt, self.nlayers, nrows, ncols)) + self.nodata
                        else:
                            outdata[var] = numpy.zeros(
                                (nt, 1, nrows, ncols)) + self.nodata
                    else:
                        print "WARNING: Variable {0} not found in output files. Skipping import.".format(var)
                prefix = set([outvars[v][0]
                              for v in outdata.keys() if v not in droughtvars])
                for c in range(len(self.lat)):
                    pdata = {}
                    for p in prefix:
                        filename = "{0}/{1}_{2:.{4}f}_{3:.{4}f}".format(
                            self.model_path, p, self.lat[c], self.lon[c], self.grid_decimal)
                        # pdata[p] = numpy.loadtxt(filename)
                        pdata[p] = pandas.read_csv(
                            filename, delim_whitespace=True, header=None).values
                    i = int((max(self.lat) + self.res /
                             2.0 - self.lat[c]) / self.res)
                    j = int((self.lon[c] - min(self.lon) +
                             self.res / 2.0) / self.res)
                    for v in outdata:
                        if v in layervars:
                            for lyr in range(self.nlayers):
                                outdata[v][:, lyr, i, j] = pdata[
                                    outvars[v][0]][:, outvars[v][1] + lyr]
                        elif v in droughtvars:
                            outdata[v][:, 0, i, j] = drought.calc(
                                v, self, int(self.gid.keys()[c]))
                        else:
                            outdata[v][:, 0, i, j] = pdata[
                                outvars[v][0]][:, outvars[v][1]]
                    print "Read output for {0}|{1}".format(self.lat[c], self.lon[c])
                dts = [date(self.startyear + self.skipyear, self.startmonth, self.startday) + timedelta(t) for t in range(nt)]
                year = [t.year for t in dts]
                month = [t.month for t in dts]
                day = [t.day for t in dts]
                outdata["date"] = numpy.array(
                    [datetime(int(year[t]), int(month[t]), int(day[t])) for t in range(len(year))])
        else:
            print("WARNING! No pixels simulated, not saving any output!")
        return outdata

    def _writeRaster(self, data, filename):
        """Writes GeoTIFF raster temporarily so that it can be imported into the database."""
        nrows, ncols = data.shape
        driver = gdal.GetDriverByName("GTiff")
        ods = driver.Create(filename, ncols, nrows, 1, gdal.GDT_Float32)
        ods.SetGeoTransform([min(self.lon) - self.res / 2.0, self.res,
                             0, max(self.lat) + self.res / 2.0, 0, -self.res])
        srs = osr.SpatialReference()
        srs.SetWellKnownGeogCS("WGS84")
        ods.SetProjection(srs.ExportToWkt())
        ods.GetRasterBand(1).WriteArray(data)
        ods.GetRasterBand(1).SetNoDataValue(self.nodata)
        ods = None

    def writeToDB(self, data, dates, tablename, initialize, ensemble=False, skipsave=0):
        """Writes output data into database."""
        db = dbio.connect(self.dbname)
        cur = db.cursor()
        if dbio.tableExists(self.dbname, self.name, tablename) and ensemble and not dbio.columnExists(self.dbname, self.name, tablename, "ensemble"):
            print("WARNING! Table {0} exists but does not contain ensemble information. Overwriting entire table!")
            cur.execute("drop table {0}.{1}".format(self.name, tablename))
            db.commit()
        if dbio.tableExists(self.dbname, self.name, tablename):
            if initialize:
                for dt in [self.startdate + timedelta(t) for t in range((self.enddate - self.startdate).days+1)]:
                    dbio.deleteRasters(self.dbname, "{0}.{1}".format(self.name, tablename), dt)
        else:
            sql = "create table {0}.{1} (id serial not null primary key, rid int not null, fdate date not null, rast raster)".format(
                self.name, tablename)
            cur.execute(sql)
            if data.shape[1] > 1:
                cur.execute("alter table {0}.{1} add column layer int".format(self.name, tablename))
            if ensemble:
                cur.execute("alter table {0}.{1} add column ensemble int".format(self.name, tablename))
            db.commit()
        startyear, startmonth, startday = self.startyear, self.startmonth, self.startday
        if skipsave > 0:
            ts = date(self.startyear, self.startmonth,
                      self.startday) + timedelta(skipsave)
            data = data[skipsave:]
            startyear, startmonth, startday = ts.year, ts.month, ts.day
        tiffiles = []
        for t in range(data.shape[0]):
            dt = date(startyear, startmonth, startday) + timedelta(t)
            for lyr in range(data.shape[1]):
                filename = "{0}/{1}_{2}{3:02d}{4:02d}_{5:02d}.tif".format(
                    self.model_path, tablename, dt.year, dt.month, dt.day, lyr + 1)
                self._writeRaster(data[t, lyr, :, :], filename)
                tiffiles.append(filename)
        ps1 = subprocess.Popen(["{0}/raster2pgsql".format(rpath.bins), "-s", "4326", "-F", "-d", "-t", "auto"] + tiffiles + ["temp"], stdout=subprocess.PIPE)
        ps2 = subprocess.Popen(["{0}/psql".format(rpath.bins), "-d", self.dbname], stdin=ps1.stdout, stdout=subprocess.PIPE)
        ps1.stdout.close()
        ps2.communicate()[0]
        ps1.wait()
        cur.execute("alter table temp add column fdate date")
        cur.execute("update temp set fdate = date (concat_ws('-',substring(filename from {0} for 4),substring(filename from {1} for 2),substring(filename from {2} for 2)))".format(
            len(tablename) + 2, len(tablename) + 6, len(tablename) + 8))
        if data.shape[1] > 1:
            cur.execute("alter table temp add column layer int")
            cur.execute("update temp set layer=(substring(filename from {0} for 2))::int".format(
                len(tablename) + 11))
        cur.execute("select count(*) from temp")
        n = int(cur.fetchone()[0])
        ntiles = n / data.shape[0]
        if data.shape[1] > 1:
            cur.execute(
                "insert into {0}.{1} (rid,fdate,layer,rast) select ((rid+{2}) % {2})+1,fdate,layer,rast from temp".format(self.name, tablename, ntiles))
        else:
            cur.execute(
                "insert into {0}.{1} (rid,fdate,rast) select ((rid+{2}) % {2})+1,fdate,rast from temp".format(self.name, tablename, ntiles))
        if bool(ensemble):
            sql = "update {0}.{1} set ensemble = {2} where ensemble is null".format(
                self.name, tablename, int(ensemble))
            cur.execute(sql)
        cur.execute("drop index if exists {0}.{1}_dtidx".format(
            self.name, tablename))
        cur.execute("create index {1}_dtidx on {0}.{1}(fdate)".format(
            self.name, tablename))
        cur.execute("drop index if exists {0}.{1}_spidx".format(
            self.name, tablename))
        cur.execute("create index {1}_spidx on {0}.{1} using gist(st_convexhull(rast))".format(
            self.name, tablename))
        db.commit()
        cur.close()
        db.close()

    def save(self, saveto, args, initialize=True, skipsave=0):
        """Reads and saves selected output data variables into the database or a user-defined directory."""
        if saveto == "db":
            output = self.readOutput(args)
            for var in output:
                if var != "date":
                    self.writeToDB(output[var], output[
                                   "date"], "{0}".format(var), initialize, skipsave=skipsave)
        else:
            if initialize:
                if os.path.isdir(saveto):
                    shutil.rmtree(saveto)
            #     elif os.path.isfile(saveto):
            #         os.remove(saveto)
            # os.makedirs(saveto)
            # shutil.move(self.model_path+"/output", saveto)
            # shutil.move(self.model_path+"/forcings", saveto)
            shutil.copytree(self.model_path, saveto,
                            ignore=shutil.ignore_patterns("*.txt"))
