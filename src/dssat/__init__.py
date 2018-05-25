""" Class definition for the DSSAT model interface

.. module:: dssat
   :synopsis: Definition of the DSSAT model class

.. moduleauthor:: Kostas Andreadis <kandread@jpl.nasa.gov>

"""

import logging
import tempfile
import decimal
import dbio
import rpath
import sys
import os
import shutil
import distutils.core
import numpy as np
import subprocess
from datetime import date, timedelta
import string


class DSSAT(object):

    def __init__(self, dbname, name, resolution, startyear, startmonth, startday,
                 endyear, endmonth, endday, nens, vicopts, shapefile=None, assimilate=True):
        log = logging.getLogger(__name__)
        self.path = tempfile.mkdtemp(dir=".")
        self.startyear = startyear
        self.startmonth = startmonth
        self.startday = startday
        self.endyear = endyear
        self.endmonth = endmonth
        self.endday = endday
        self.crop = None
        self.cultivars = {}
        self.lat = []
        self.lon = []
        self.elev = []
        self.depths = []
        self.dbname = dbname
        self.name = name
        self.res = resolution
        self.nens = nens
        self.shapefile = shapefile
        self.assimilate = assimilate
        self.modelpaths = {}
        self.modelstart = {}
        self.grid_decimal = - (decimal.Decimal(str(self.res)).as_tuple().exponent - 1)
        db = dbio.connect(self.dbname)
        cur = db.cursor()
        if 'lai' in vicopts or ('save' in vicopts and vicopts['save'].find("lai") >= 0):
            self.lai = "vic"
        else:
            self.lai = None
        if 'save to' in vicopts:
            self.datafrom = vicopts['save to']
        else:
            self.datafrom = "db"
        cur.execute(
            "select * from information_schema.tables where table_name='basin' and table_schema=%s", (name,))
        if not bool(cur.rowcount):
            log.error("No simulation named {0} exists in database. You might have to run VIC.".format(name))
            sys.exit()
        cur.execute(
            'select basefile from vic.input where resolution=%f;' % self.res)
        self.basefile = "{0}/{1}".format(rpath.data, cur.fetchone()[0])
        cur.close()
        db.close()

    def readVICSoil(self):
        """Extract information from VIC database table on latitude, longitude,
        elevation  and soil depths."""
        db = dbio.connect(self.dbname)
        cur = db.cursor()
        sql = "select st_y(geom), st_x(geom), elev, depths from {0}.basin".format(
            self.name)
        cur.execute(sql)
        pixels = cur.fetchall()
        self.lat, self.lon, self.elev, self.depths = zip(*pixels)
        self.lat = np.array(self.lat)
        self.lon = np.array(self.lon)
        self.elev = np.array(self.elev)
        self.depths = list(self.depths)
        cur.close()
        db.close()

    def writeWeatherFiles(self, modelpath, name, year, month, day, weather, elev, lat, lon, ts=None, te=None):
        """Writes ensemble weather files for specific pixel."""
        if isinstance(weather, list):
            data = (weather * (int(self.nens / len(weather)) + 1))[:self.nens]
        else:
            data = [weather] * self.nens
        for ens in range(self.nens):
            filename = "{0}/WEATH{1:03d}.WTH".format(modelpath, ens + 1)
            fout = open(filename, 'w')
            fout.write("*WEATHER DATA : {0}\r\n".format(name[:5].upper()))
            fout.write("\r\n")
            fout.write("@ INSI LAT LONG ELEV TAV AMP REFHT WNDHT\r\n")
            tavg = np.mean(data[ens][:, 1:3])
            fout.write("{0:6s} {1} {2} {3:.0f} {4:.1f} {5:.1f} {6:.1f} {7:.1f} \r\n".format(
                name[:5].upper(), lat, lon, elev, tavg, -99.0, -99.0, -99.0))
            fout.write("@DATE SRAD TMAX TMIN RAIN DEWP WIND PAR\r\n")
            if ts is None or te is None:
                ts = 0
                te = len(data[ens])
            for p in range(ts, te):
                datestr = str(int(year[p]))[-2:] + date(int(year[p]),
                                                        int(month[p]), int(day[p])).strftime("%j")
                fout.write("{0}  {1:4.1f}  {2:4.1f}  {3:4.1f}  {4:4.1f}\r\n".format(
                    datestr, data[ens][p, 0] * 0.086400, data[ens][p, 1], data[ens][p, 2], data[ens][p, 3]))
            fout.close()

    def readVICOutputFromFile(self, lat, lon, depths, filespath):
        """Read DSSAT inputs from VIC output files for a specific pixel."""
        startdate = date(self.startyear, self.startmonth, self.startday)
        enddate = date(self.endyear, self.endmonth, self.endday)
        filename = "{0}/output/eb_{1:.{3}f}_{2:.{3}f}".format(
            filespath, lat, lon, self.grid_decimal)
        viceb = np.loadtxt(filename)
        filename = "{0}/output/sub_{1:.{3}f}_{2:.{3}f}".format(
            filespath, lat, lon, self.grid_decimal)
        vicsm = np.loadtxt(filename)
        filename = "{0}/output/sur_{1:.{3}f}_{2:.{3}f}".format(
            filespath, lat, lon, self.grid_decimal)
        vicsr = np.loadtxt(filename)
        filename = "{0}/forcings/data_{1:.{3}f}_{2:.{3}f}".format(
            filespath, lat, lon, self.grid_decimal)
        met = np.loadtxt(filename)
        sm = vicsm[:, 3:len(depths) + 3]
        weather = np.vstack(
            (viceb[:, 3] + viceb[:, 4], met[:, 1], met[:, 2], met[:, 0])).T
        year = vicsm[:, 0].astype(int)
        month = vicsm[:, 1].astype(int)
        day = vicsm[:, 2].astype(int)
        tidx = [i for i in range(len(year)) if date(year[i], month[i], day[
            i]) >= startdate and date(year[i], month[i], day[i]) <= enddate]
        lai = dict(zip([date(year[i], month[i], day[i])
                        for i in range(len(year)) if i in tidx], vicsr[:, 12]))
        return year[tidx], month[tidx], day[tidx], weather[tidx, :], sm[tidx, :], lai

    def readVICOutputFromDB(self, gid, depths):
        """Read DSSAT inputs from database."""
        startdate = date(self.startyear, self.startmonth, self.startday)
        enddate = date(self.endyear, self.endmonth, self.endday)
        ndays = (enddate - startdate).days + 1
        db = dbio.connect(self.dbname)
        cur = db.cursor()
        date_sql = "fdate>=date '{0}-{1}-{2}' and fdate<=date '{3}-{4}-{5}'".format(
            self.startyear, self.startmonth, self.startday, self.endyear, self.endmonth, self.endday)
        data = {}
        varnames = ["net_short", "net_long",
                    "soil_moist", "rainf", "tmax", "tmin"]
        if self.lai is not None:
            varnames.append("lai")
        else:
            lai = None
        for varname in varnames:
            sqlvars = ["fdate"]
            sql = "select column_name from information_schema.columns where table_schema='{0}' and table_name='{1}' and column_name='ensemble'".format(
                self.name, varname)
            cur.execute(sql)
            if bool(cur.rowcount):
                sqlvars += ["ensemble"]
            sql = "select column_name from information_schema.columns where table_schema='{0}' and table_name='{1}' and column_name='layer'".format(
                self.name, varname)
            cur.execute(sql)
            if bool(cur.rowcount):
                sqlvars += ["layer"]
            sql = "select {0}, avg((st_summarystats(rast)).mean) from {1}.{2}, {1}.agareas where st_intersects(rast,geom) and gid={3} and {4} group by gid,{0} order by fdate".format(
                string.join(sqlvars, ","), self.name, varname, gid, date_sql)
            cur.execute(sql)
            if bool(cur.rowcount):
                results = cur.fetchall()
                if "ensemble" in sqlvars:
                    vicnens = np.max([r[1] for r in results])
                    data[varname] = [np.array(
                        [r[-1] for r in results if r[1] == ens + 1]) for ens in range(vicnens)]
                    if "layer" in sqlvars:
                        layers = np.array([r[2] for r in results if r[1] == 1])
                        nlayers = np.max(layers)
                    else:
                        year = np.array([r[0].year for r in results if r[1] == 1])
                        month = np.array([r[0].month for r in results if r[1] == 1])
                        day = np.array([r[0].day for r in results if r[1] == 1])
                else:
                    data[varname] = np.array([r[-1] for r in results])
                    if "layer" in sqlvars:
                        layers = np.array([r[1] for r in results])
                        nlayers = np.max(layers)
                    else:
                        year = np.array([r[0].year for r in results])
                        month = np.array([r[0].month for r in results])
                        day = np.array([r[0].day for r in results])
                assert len(year) == ndays and len(month) == ndays and len(day) == ndays
        cur.close()
        db.close()
        if "ensemble" in sqlvars:
            weather = [np.vstack((data["net_short"][e] - data["net_long"][e], data["tmax"][
                                 e], data["tmin"][e], data["rainf"][e])).T for e in range(len(data["net_short"]))]
            sm = [np.zeros((len(year), nlayers))] * len(data["soil_moist"])
            if self.lai is not None:
                lai = dict(zip([date(year[i], month[i], day[i]) for i in range(
                    len(year))], np.mean(np.array(data["lai"]).T, axis=1)))
            for e in range(len(sm)):
                for l in range(nlayers):
                    sm[e][:, l] = [m for mi, m in enumerate(
                        data["soil_moist"][e]) if layers[mi] == l + 1]
        else:
            weather = np.vstack(
                (data["net_short"] - data["net_long"], data["tmax"], data["tmin"], data["rainf"])).T
            if self.lai is not None:
                lai = dict(zip([date(year[i], month[i], day[i])
                                for i in range(len(year))], np.array(data["lai"]).T))
            sm = np.zeros((len(year), nlayers))
            for l in range(nlayers):
                sm[:, l] = [m for mi, m in enumerate(
                    data["soil_moist"]) if layers[mi] == l + 1]
        return year, month, day, weather, sm, lai

    def readVICOutput(self, gid, depths):
        """Reads DSSAT time-varying inputs by reading either from files or a database."""
        log = logging.getLogger(__name__)
        if isinstance(self.datafrom, list):
            inputs = []
            while len(inputs) < self.nens:
                inputs += self.datafrom
            inputs = inputs[:self.nens]
            lat, lon = self.gid[gid]
        if self.datafrom == 'db':
            year, month, day, weather, sm, lai = self.readVICOutputFromDB(
                gid, depths)
        else:
            log.error("VIC output was not saved in the database. Cannot proceed with the DSSAT simulation.")
            sys.exit()
        return year, month, day, weather, sm, lai

    def writeLAI(self, modelpath, gid, viclai=None, tablename="lai.modis"):
        """Writes LAI file for DSSAT."""
        fout = open("{0}/LAI.txt".format(modelpath), 'w')
        db = dbio.connect(self.dbname)
        cur = db.cursor()
        cur.execute("select * from information_schema.tables where table_name=%s and table_schema='lai'",
                    (tablename.split(".")[1],))
        if bool(cur.rowcount) and not self.lai == "vic":
            sql = "select fdate,avg((st_summarystats(st_clip(rast,geom))).mean) from {0},{1}.agareas where st_intersects(rast,geom) and fdate>=date '{2}-{3}-{4}' and fdate<=date '{5}-{6}-{7}' and gid={8} group by fdate".format(
                tablename, self.name, self.startyear, self.startmonth, self.startday, self.endyear, self.endmonth, self.endday, gid)
            cur.execute(sql)
            if bool(cur.rowcount):
                results = cur.fetchall()
                lai = {}
                for r in results:
                    if r[1] is None:
                        lai[r[0]] = -9999.0
                    else:
                        lai[r[0]] = r[1] / 10.0
            else:
                lai = {}
        else:
            lai = viclai
        enddate = date(self.endyear, 12, 31)
        startdate = date(self.startyear, 1, 1)
        for t in range((enddate - startdate).days + 1):
            dt = startdate + timedelta(t)
            if lai is not None and dt in lai:
                fout.write("{0:.1f}\n".format(lai[dt]))
            else:
                fout.write("-9999.0\n")
        fout.close()
        cur.close()
        db.close()

    def writeSoilMoist(self, modelpath, year, month, day, smi, dz):
        """Writes soil moisture information file."""
        filename = "{0}/SOIL_MOISTURE.ASC".format(modelpath)
        fout = open(filename, 'w')
        ndays = (date(year[0] + 1, 1, 1) - date(year[0], 1, 1)).days
        tv = 0
        for t in range(ndays):
            dt = date(year[0], 1, 1) + timedelta(t)
            doy = int(dt.strftime("%j"))
            fout.write("{0:.0f} {1:.0f} {2:.0f} ".format(
                dt.year, dt.month, dt.day))
            if tv < len(year) and dt == date(int(year[tv]), int(month[tv]), int(day[tv])):
                for lyr in range(len(dz)):
                    fout.write("{0:.3f} ".format(smi[tv, lyr]))
                tv += 1
            else:
                for lyr in range(len(dz)):
                    fout.write("{0:.0f} ".format(-9999.0))
            fout.write("{0}\n".format(doy))
        fout.close()

    def sampleSoilProfiles(self, gid):
        """Samples soil profiles from database to be used in DSSAT control file."""
        db = dbio.connect(self.dbname)
        cur = db.cursor()
        sql = "with f as (select st_envelope(geom) as geom from {0}.agareas where gid={1}) select props from dssat.soils as s,f where st_intersects(s.geom,f.geom)".format(self.name, gid)
        cur.execute(sql)
        # if crop area is too small, look for nearest soil profiles
        dist = 0.1
        while not bool(cur.rowcount):
            sql = "with a as (select st_buffer(geom,{2}) as geom from {0}.agareas where gid={1}) select props from dssat.soils as s,a where st_intersects(s.geom,a.geom)".format(
                self.name, gid, dist)
            dist += 0.1
            cur.execute(sql)
        profiles = cur.fetchall()
        ens = np.random.choice(range(len(profiles)), self.nens)
        cur.close()
        db.close()
        return [profiles[e] for e in ens]

    def writeConfigFile(self, modelpath, nlayers, startdate, enddate):
        """Write DSSAT-ENKF config file."""
        configfilename = "ENKF_CONFIG.TXT"
        fout = open("{0}/{1}".format(modelpath, configfilename), 'w')
        fout.write("!Start_DOY_of_Simulation:\n{0}\n".format(
            int(startdate.strftime("%j"))))
        fout.write("!End_DOY_of_Simulation\n{0}\n".format(
            int(enddate.strftime("%j"))))
        fout.write("!Year_of_Simulation:\n{0}\n".format(startdate.year))
        fout.write("!Ensemble_members\n{0}\n".format(self.nens))
        fout.write("!Number_of_soil_layers\n{0}\n".format(nlayers))
        ndays = (date(self.endyear, 12, 31) - date(self.startyear, 1, 1)).days
        fout.write("!Number_of_RS_data\n{0}".format(ndays))
        fout.close()
        return configfilename

    def calcCroplandFract(self):
        """Calculate fraction of cropland for specific pixel."""
        db = dbio.connect(self.dbname)
        cur = db.cursor()
        sql = "select gid,avg((st_summarystats(st_clip(rast,geom))).mean) from dssat.cropland,{0}.agareas where st_intersects(rast,geom) group by gid order by gid".format(
            self.name)
        cur.execute(sql)
        fract = dict((r[0], r[1]) for r in cur.fetchall())
        cur.close()
        db.close()
        return fract

    def readShapefile(self):
        """Read areas from shapefile where DSSAT will be run."""
        log = logging.getLogger(__name__)
        try:
            cmd = "{0}/shp2pgsql -s 4326 -d -I -g geom {1} {2}.agareas | {0}/psql -d {3}".format(rpath.bins, self.shapefile, self.name, self.dbname)
            subprocess.call(cmd, shell=True)
            db = dbio.connect(self.dbname)
            cur = db.cursor()
            sql = "select gid, st_x(st_centroid(geom)), st_y(st_centroid(geom)) from {0}.agareas".format(self.name)
            cur.execute(sql)
            geoms = cur.fetchall()
            return geoms
        except IOError:
            log.error("Shapefile {0} for DSSAT simulation does not exist. Exiting...".format(
                self.shapefile))
            sys.exit()

    def planting(self, lat, lon, fromShapefile=False):
        """Retrieve planting dates for pixel."""
        if self.crop is None:
            self.crop = "maize"
        db = dbio.connect(self.dbname)
        cur = db.cursor()
        sql = "select st_value(rast,st_geomfromtext('POINT({0} {1})',4326)) as doy from crops.plantstart where type like '{2}' and st_intersects(rast,st_geomfromtext('POINT({0} {1})',4326)) order by doy".format(
            lon, lat, self.crop)
        cur.execute(sql)
        results = cur.fetchall()
        plantdates = [date(self.startyear, 1, 1) + timedelta(r[0] - 1) for r in results if r[0] is not None]
        cur.close()
        db.close()
        startdt = date(self.startyear, self.startmonth, self.startday)
        planting = [p for p in plantdates if p >= startdt and p <= date(self.endyear, self.endmonth, self.endday)]
        if planting is []:
            planting = [plantdates[np.argmax([(t - startdt).days for t in plantdates if (t - startdt).days < 0])]]
        return planting

    def interpolateSoilMoist(self, sm, depths, dz):
        """Estimate soil moisture at DSSAT depths."""
        sm_i = []
        if len(sm.shape) < 2:
            sm = np.reshape(sm, (1, len(sm)))
        for t in range(sm.shape[0]):
            u = sm[t, :] / np.array(depths * 1000.0)
            z = [100.0 * depths[0] / 2.0]
            for lyr in range(1, len(u)):
                # midpoint of each layer in cm
                z.append(100.0 * (depths[lyr - 1] + depths[lyr] / 2.0))
            dz1 = [0.0] + list(dz)
            znew = np.array([dz1[i] + (dz1[i + 1] - dz1[i]) /
                             2.0 for i in range(len(dz1) - 1)])
            unew = np.interp(znew, z, u)
            sm_i.append(unew)
        return np.array(sm_i)

    def copyModelFiles(self, geom, pi, dssatexe):
        """Copy DSSAT model files to instance's directory."""
        gid, lat, lon = geom
        modelpath = os.path.abspath("{0}/{1}_{2}_{3}".format(self.path, lat, lon, pi))
        self.modelpaths[(gid, pi)] = modelpath
        os.mkdir(modelpath)
        os.mkdir(modelpath + "/ENKF_Results")
        shutil.copyfile("{0}/{1}".format(rpath.bins, dssatexe), "{0}/{1}".format(modelpath, dssatexe))
        distutils.dir_util.copy_tree("{0}/dssat".format(rpath.data), modelpath)

    def setupModelInstance(self, geom, dssatexe):
        """Setup parameters and write input files for a DSSAT model instance
        over a specific geometry."""
        log = logging.getLogger(__name__)
        gid, lon, lat = geom
        c = np.argmin(np.sqrt((lat - self.lat) **
                              2 + (lon - self.lon) ** 2))
        # use the soil depths from the nearest VIC pixel to the centroid
        depths = np.array(self.depths[c])
        year, month, day, weather, sm, vlai = self.readVICOutput(gid, depths)
        vicstartdt = date(year[0], month[0], day[0])
        planting = self.planting(lat, lon)
        for pi, pdt in enumerate(planting[:1]):
            self.copyModelFiles(geom, pi, dssatexe)
            try:
                if pdt > date(pdt.year, 1, 8):
                    simstartdt = pdt - timedelta(7)
                else:
                    simstartdt = pdt
                assert simstartdt >= vicstartdt
                modelpath = self.modelpaths[(gid, pi)]
                self.modelstart[(gid, pi)] = simstartdt
                dz, smi = self.writeControlFile(modelpath, sm, depths, simstartdt, gid, self.lat[c], self.lon[c], pdt, None, None)
                ti0 = [i for i in range(len(year)) if simstartdt == date(year[i], month[i], day[i])][0]
                if pi + 1 < len(planting):
                    ti1 = [i for i in range(len(year)) if (planting[pi + 1] - timedelta(10)) == date(year[i], month[i], day[i])][0]
                else:
                    ti1 = [i for i in range(len(year)) if (planting[pi] + timedelta(min(180, len(year) - (planting[pi] - date(self.startyear - 1, 12, 31)).days))) == date(year[i], month[i], day[i])][0]
                self.writeWeatherFiles(modelpath, self.name, year, month, day, weather, self.elev[c], self.lat[c], self.lon[c])  #, ti0, ti1)
                self.writeSoilMoist(modelpath, year, month, day, smi, dz)
                self.writeLAI(modelpath, gid, viclai=vlai)
                self.writeConfigFile(modelpath, smi.shape[1], simstartdt, date(year[ti1], month[ti1], day[ti1]))
                log.info("Wrote DSSAT for planting date {0}".format(pdt.strftime("%Y-%m-%d")))
            except AssertionError:
                log.error("No input data for DSSAT corresponding to starting date {0}. Need to run VIC for these dates. Exiting...".format(simstartdt.strftime('%Y-%m-%d')))

    def runModelInstance(self, modelpath, dssatexe):
        """Runs DSSAT model instance."""
        log = logging.getLogger(__name__)
        os.chdir(modelpath)
        if bool(self.assimilate):
            if str(self.assimilate).lower() is "sm":
                sm_assim = "Y"
                lai_assim = "N"
            elif str(self.assimilate).lower() is "lai":
                sm_assim = "N"
                lai_assim = "Y"
            else:
                sm_assim = lai_assim = "Y"
        else:
            sm_assim = lai_assim = "N"
        proc = subprocess.Popen(["wine", dssatexe, "SOIL_MOISTURE.ASC", "LAI.txt", "SM{0}".format(sm_assim), "LAI{0}".format(lai_assim)])
        out, err = proc.communicate()
        log.debug(out)

    def save(self):
        """Saves DSSAT output to database."""
        db = dbio.connect(self.dbname)
        cur = db.cursor()
        cur.execute(
            "select * from information_schema.tables where table_name='dssat' and table_schema='{0}'".format(self.name))
        if not bool(cur.rowcount):
            cur.execute("create table {0}.dssat (id serial primary key, gid int, ensemble int, fdate date, wsgd real, lai real, gwad real, geom geometry, CONSTRAINT enforce_dims_geom CHECK (st_ndims(geom) = 2), CONSTRAINT enforce_geotype_geom CHECK (geometrytype(geom) = 'POLYGON'::text OR geometrytype(geom) = 'MULTIPOLYGON'::text OR geom IS NULL))".format(self.name))
            db.commit()
        # overwrite overlapping dates
        cur.execute("delete from {0}.dssat where fdate>=date'{1}-{2}-{3}' and fdate<=date'{4}-{5}-{6}'".format(self.name, self.startyear, self.startmonth, self.startday, self.endyear, self.endmonth, self.endday))
        sql = "insert into {0}.dssat (fdate, gid, ensemble, gwad, wsgd, lai) values (%(dt)s, %(gid)s, %(ens)s, %(gwad)s, %(wsgd)s, %(lai)s)".format(self.name)
        for gid, pi in self.modelpaths:
            modelpath = self.modelpaths[(gid, pi)]
            startdt = self.modelstart[(gid, pi)]
            for e in range(self.nens):
                with open("{0}/PLANTGRO{1:03d}.OUT".format(modelpath, e + 1)) as fin:
                    line = fin.readline()
                    while line.find("YEAR") < 0:
                        line = fin.readline()
                    for line in fin:
                        data = line.split()
                        dt = date(startdt.year, 1, 1) + \
                            timedelta(int(data[1]) - 1)
                        dts = "{0}-{1}-{2}".format(dt.year, dt.month, dt.day)
                        if self.cultivars[gid][e] is None:
                            cultivar = ""
                        else:
                            cultivar = self.cultivars[gid][e]
                        if float(data[9]) > 0.0:
                            cur.execute(sql, {'dt': dts, 'ens': e + 1, 'gwad': float(
                                data[9]), 'wsgd': float(data[18]), 'lai': float(data[6]), 'gid': gid, 'cultivar': cultivar})
        cur.execute(
            "update {0}.dssat as d set geom = a.geom from {0}.agareas as a where a.gid=d.gid".format(self.name))
        db.commit()
        cur.execute("drop index if exists {0}.d_t".format(self.name))
        cur.execute("drop index if exists {0}.d_s".format(self.name))
        cur.execute(
            "create index d_t on {0}.dssat(fdate)".format(self.name))
        cur.execute(
            "create index d_s on {0}.dssat using gist(geom)".format(self.name))
        db.commit()
        cur.close()
        db.close()
        self.yieldTable()

    def yieldTable(self):
        """Create table for crop yield statistics."""
        fsql = "with f as (select gid,geom,gwad,ensemble,fdate from (select gid,geom,gwad,ensemble,fdate,row_number() over (partition by gid,ensemble order by gwad desc) as rn from {0}.dssat) gwadtable where rn=1)".format(self.name)
        db = dbio.connect(self.dbname)
        cur = db.cursor()
        cur.execute(
            "select * from information_schema.tables where table_name='yield' and table_schema='{0}'".format(self.name))
        if not bool(cur.rowcount):
            sql = "create table {0}.yield as ({1} select gid,geom,max(gwad) as max_yield,avg(gwad) as avg_yield,stddev(gwad) as std_yield,max(fdate) as fdate from f group by gid,geom)".format(self.name, fsql)
            cur.execute(sql)
            cur.execute("alter table {0}.yield add column crop text".format(self.name))
            cur.execute("alter table {0}.yield add primary key (gid)".format(self.name))
        else:
            cur.execute("delete from {0}.yield where fdate>='{1}-{2}-{3}' and fdate<='{4}-{5}-{6}'".format(self.name, self.startyear, self.startmonth, self.startday, self.endyear, self.endmonth, self.endday))
            sql = "insert into {0}.yield ({1} select gid,geom,max(gwad) as max_yield,avg(gwad) as avg_yield,stddev(gwad) as std_yield,max(fdate) as fdate from f group by gid,geom)".format(self.name, fsql)
            cur.execute(sql)
        db.commit()
        cur.execute("update {0}.yield set std_yield = 0 where std_yield is null".format(self.name))
        cur.execute("drop index if exists {0}.yield_s".format(self.name))
        db.commit()
        cur.execute("create index yield_s on {0}.yield using gist(geom)".format(self.name))
        cur.close()
        db.close()

    def run(self, dssatexe="DSSAT_EnKF.exe", crop_threshold=0.1):
        """Runs DSSAT simulation."""
        self.readVICSoil()
        geoms = self.readShapefile()
        cropfract = self.calcCroplandFract()
        for geom in geoms:
            gid = geom[0]
            if cropfract[gid] >= crop_threshold:
                self.setupModelInstance(geom, dssatexe)
        for k in self.modelpaths:
            modelpath = self.modelpaths[k]
            self.runModelInstance(modelpath, dssatexe)
        self.save()
