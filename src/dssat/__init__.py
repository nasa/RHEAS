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
import numpy as np
from datetime import date, timedelta
import string


class DSSAT:

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
        try:
            self.grid_decimal = - \
                (decimal.Decimal(self.res).as_tuple().exponent - 1)
        except TypeError:
            self.grid_decimal = - \
                (decimal.Decimal(str(self.res)).as_tuple().exponent - 1)
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
            fout.write("*WEATHER DATA : {0}\n".format(name[:5].upper()))
            fout.write("\n")
            fout.write("@ INSI LAT LONG ELEV TAV AMP REFHT WNDHT\n")
            tavg = np.mean(data[ens][:, 1:3])
            fout.write("{0:6s} {1} {2} {3:.0f} {4:.1f} {5:.1f} {6:.1f} {7:.1f} \n".format(
                name[:5].upper(), lat, lon, elev, tavg, -99.0, -99.0, -99.0))
            fout.write("@DATE SRAD TMAX TMIN RAIN DEWP WIND PAR\n")
            if ts is None or te is None:
                ts = 0
                te = len(data[ens])
            for p in range(ts, te):
                datestr = str(int(year[p]))[-2:] + date(int(year[p]),
                                                        int(month[p]), int(day[p])).strftime("%j")
                fout.write("{0}  {1:4.1f}  {2:4.1f}  {3:4.1f}  {4:4.1f}\n".format(
                    datestr, data[ens][p, 0] * 0.086400, data[ens][p, 1], data[ens][p, 2], data[ens][p, 3]))
            fout.close()

    def _readVICOutputFromFile(self, lat, lon, depths, filespath):
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

    def _readVICOutputFromDB(self, gid, depths):
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
            weather = [np.vstack((data["net_short"][e] + data["net_long"][e], data["tmax"][
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
                (data["net_short"] + data["net_long"], data["tmax"], data["tmin"], data["rainf"])).T
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
            year, month, day, weather, sm, lai = self._readVICOutputFromDB(
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

    def _sampleSoilProfiles(self, gid):
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

    def _calcCroplandFract(self):
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


