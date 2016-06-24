""" Class definition for the DSSAT model interface

.. module:: dssat
   :synopsis: Definition of the DSSAT model class

.. moduleauthor:: Kostas Andreadis <kandread@jpl.nasa.gov>

"""

import distutils.core
import sys
import os
import subprocess
import tempfile
import string
import decimal
import shutil
import numpy as np
from datetime import date, timedelta
import netCDF4 as netcdf
import multiprocessing
from collections import OrderedDict
import rpath
import random
import dbio


def addCultivar(dbname, shapefile, params, nens=40, crop="maize"):
    """Add cultivar parameters to the database *dbname* corresponding
    to the area defined in the *shapefile*. The *params* is a list of dictionaries,
    where the keys of each dictionary correspond to parameters, and each object in
    the list corresponds to a cultivar variant. The *nens* parameters is the size
    of the ensemble to be created."""
    temptable = ''.join(random.SystemRandom().choice(
        string.ascii_letters) for _ in range(8))
    if os.path.exists(shapefile):
        subprocess.call("{0}/shp2pgsql -d -s 4326 -g geom {1} {2} | {0}/psql -d {3}".format(
            rpath.bins, shapefile, temptable, dbname), shell=True)
        db = dbio.connect(dbname)
        cur = db.cursor()
        e = 0
        while e < nens:
            for c in range(len(params)):
                if crop == "maize" and all(p in params[c] for p in ['p1', 'p2', 'p5', 'g2', 'g3', 'phint']):
                    if e < nens:
                        sql = "insert into dssat.cultivars (geom) (select geom from {0})".format(
                            temptable)
                        cur.execute(sql)
                        sql = "update dssat.cultivars set ensemble={0},{1} where ensemble is null".format(
                            e + 1, ",".join(["{0}={1}".format(k, params[c][k]) for k in params[c]]))
                        cur.execute(sql)
                        e += 1
                else:
                    print("Missing parameters for {0} crop".format(crop))
                    params.pop(c)  # remove element with missing parameters
                    break
        cur.execute("drop table {0}".format(temptable))
        db.commit()
        cur.close()
        db.close()
    else:
        print(
            "Shapefile {0} cannot be found. Not adding cultivars!".format(shapefile))


def _run1(modelpath, exe, assimilate="Y"):
    """Runs DSSAT simulation for individual pixel."""
    os.chdir(modelpath)
    # devnull = open(os.devnull, 'wb')
    # subprocess.call("wine {0} SOIL_MOISTURE.ASC LAI.txt SM{1} LAI{1}".format(exe, assimilate), shell=True)#, stdout=subprocess.PIPE, stderr=devnull)
    # devnull.close()
    subprocess.call(["wine", exe, "SOIL_MOISTURE.ASC", "LAI.txt",
                     "SM{0}".format(assimilate), "LAI{0}".format(assimilate)])


class DSSAT:

    def __init__(self, dbname, name, resolution, startyear, startmonth, startday,
                 endyear, endmonth, endday, nens, vicopts, shapefile=None, assimilate="Y"):
        self.path = tempfile.mkdtemp(dir=".")
        self.startyear = startyear
        self.startmonth = startmonth
        self.startday = startday
        self.endyear = endyear
        self.endmonth = endmonth
        self.endday = endday
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
        except:
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
            print "ERROR! No simulation named {0} exists in database. You might have to run VIC.".format(name)
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
            # filename = self.path+"/WEATH{0:03d}.WTH".format(ens+1)
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

    def _readVICOutputFromDB(self, gid, depths):  # lat, lon, depths):
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
        # else:
        #     print("Error! VIC simulation does not contain any data. Exiting...")
        #     sys.exit()
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

    def _readVICOutputFromNetCDF(self, lat, lon, depths, ncfilename):
        """Reads VIC output from a NetCDF file."""
        try:
            ncfile = netcdf.Dataset(ncfilename)
        except:
            print("Could not find NetCDF file {0}.".format(ncfilename))
            sys.exit()
        try:
            ensemble = bool(ncfile.getncattr("ensemble"))
        except:
            ensemble = False
        t = ncfile.variables['time']
        dt = netcdf.num2date(t[:].astype('int'), units=t.units)
        year = np.array([tt.year for tt in dt])
        month = np.array([tt.month for tt in dt])
        day = np.array([tt.day for tt in dt])
        lats = ncfile.variables['lat'][:]
        lons = ncfile.variables['lon'][:]
        i = np.where(lats == lat)[0][0]
        j = np.where(lons == lon)[0][0]
        net_short = ncfile.variables['net_short'][:]
        net_long = ncfile.variables['net_long'][:]
        tmax = ncfile.variables['tmax'][:]
        tmin = ncfile.variables['tmin'][:]
        rainf = ncfile.variables['rainf'][:]
        soilmoist = ncfile.variables['soil_moist'][:]
        if ensemble:
            nens = net_short.shape[1]
            weather = [np.vstack((net_short[:, e, i, j] + net_long[:, e, i, j], tmax[
                                 :, e, i, j], tmin[:, e, i, j], rainf[:, e, i, j])).T for e in range(nens)]
            sm = [soilmoist[:, e, :, i, j] for e in range(nens)]
            lai = dict(
                zip(dt, np.mean(ncfile.variables['lai'][:, :, i, j], axis=1)))
        else:
            weather = np.vstack((net_short[
                                :, i, j] + net_long[:, i, j], tmax[:, i, j], tmin[:, i, j], rainf[:, i, j])).T.data
            sm = soilmoist[:, :, i, j]
            lai = dict(zip(dt, ncfile.variables['lai'][:, i, j]))
        ncfile.close()
        return year, month, day, weather, sm, lai

    def readVICOutput(self, gid, depths):
        """Reads DSSAT time-varying inputs by reading either from files or a database."""
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
            print(
                "VIC output was not saved in the database. Cannot proceed with the DSSAT simulation.")
            sys.exit()
        return year, month, day, weather, sm, lai

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

    def writeControlFile(self, modelpath, vsm, depths, startdate, gid, lat, lon, planting, fertildates, irrigdates):
        """Writes DSSAT control file for specific pixel."""
        if isinstance(vsm, list):
            vsm = (vsm * (int(self.nens / len(vsm)) + 1))[:self.nens]
        else:
            vsm = [vsm] * self.nens
        try:
            fin = open(self.basefile)
            blines = fin.readlines()
        except IOError:
            print "ERROR opening {0}".format(self.basefile)
            sys.exit()
        profiles = self._sampleSoilProfiles(gid)
        profiles = [p[0] for p in profiles]
        for ens in range(self.nens):
            sm = vsm[ens]
            fertilizers = fertildates
            irrigation = irrigdates
            prof = profiles[ens].split("\n")
            dz = map(lambda ln: float(ln.split()[0]), profiles[
                     ens].split("\n")[3:-1])
            smi = self._interpolateSoilMoist(sm, depths, dz)
            filename = "{0}/DSSAT{1}_{2:03d}.INP" .format(
                modelpath, self.nens, ens + 1)
            fout = open(filename, 'w')
            l = 0
            while l < len(blines):
                line = blines[l]
                if line.find("SIMULATION CONTROL") > 0:
                    fout.write(blines[l])
                    l += 1
                    dt = blines[l].split()[3]
                    fout.write(blines[l].replace(dt, "{0:04d}{1:03d}".format(
                        startdate.year, (startdate - date(startdate.year, 1, 1)).days + 1)))
                    l += 1
                elif line.find("FIELDS") > 0:
                    fout.write(blines[l])
                    fout.write(blines[l + 1])
                    blat = blines[l + 2].split()[0]
                    blon = blines[l + 2].split()[1]
                    fout.write(blines[
                               l + 2].replace(blat, "{0:8.5f}".format(lat)).replace(blon, "{0:10.5f}".format(lon)))
                    l += 3
                elif line.find("AUTOMATIC") > 0:
                    fout.write(blines[l])
                    dt1 = blines[l + 1].split()[0]
                    dt2 = blines[l + 1].split()[1]
                    # self.startyear, self.startmonth, self.startday)
                    dt = date(2012, 1, 1)
                    dts = "{0:04d}{1}".format(dt.year, dt.strftime("%j"))
                    fout.write(
                        blines[l + 1].replace(dt1, dts).replace(dt2, dts))
                    fout.write(blines[l + 2])
                    fout.write(blines[l + 3])
                    fout.write(blines[l + 4])
                    dt1 = blines[l + 5].split()[1]
                    fout.write(blines[l + 5].replace(dt1, dts))
                    l += 6
                elif line.find("INITIAL CONDITIONS") > 0:
                    fout.write(blines[l])
                    dt = blines[l + 1].split()[1]
                    fout.write(blines[l + 1].replace(dt, "{0:04d}{1:03d}".format(
                        startdate.year, int(startdate.strftime("%j")))))
                    l += 2
                    for lyr in range(len(dz)):
                        fout.write("{0:8.0f}{1:8.3f}{2:8.1f}{3:8.1f}\n".format(
                            dz[lyr], smi[0, lyr], 0.5, 0.1))
                    while blines[l].find("PLANTING") < 0:
                        l += 1
                elif line.find("WEATHER") == 0:
                    oldfile = line.split()[1]
                    fout.write(blines[l].replace(
                        oldfile, "WEATH{0:03d}.WTH" .format(ens + 1)))
                    l += 1
                elif line.find("PLANTING") >= 0:
                    fout.write(line)
                    toks = blines[l + 1].split()
                    olddate = toks[0]
                    try:
                        assert planting >= date(self.startyear, self.startmonth, self.startday) and planting <= date(
                            self.endyear, self.endmonth, self.endday)
                    except:
                        print("Planting date selected outside simulation period.")
                        sys.exit()
                    dts = "{0:04d}{1}".format(
                        planting.year, planting.strftime("%j"))
                    fout.write(blines[l + 1].replace(olddate, dts))
                    l += 2
                elif line.find("OUTPUT") == 0:
                    oldfile = line.split()[1]
                    fout.write(blines[l].replace(
                        oldfile, "OUTPT{0:03d}" .format(ens + 1)))
                    l += 1
                elif line.find("SOIL") > 0:
                    fout.write(blines[l])
                    for ln in range(len(prof) - 1):
                        fout.write(prof[ln] + "\n")
                    fout.write("\n")
                    for z in dz:
                        fout.write(
                            "{0:6.0f}   0.0   0.0   0.0   0.0   0.0   0.0   0.0   0.0   0.0   0.0   0.0   0.0   0.0   0.0   0.0\n".format(z))
                    while blines[l].find("CULTIVAR") < 0:
                        l += 1
                    l -= 1
                elif line.find("CULTIVAR") > 0:
                    fout.write(line)
                    if len(blines[l + 1].split()) < 5:
                        fout.write(blines[l + 1])
                    else:
                        cultivar = self._cultivar(ens, gid)  # lat, lon)
                        fout.write(cultivar + "\n")
                    l += 2
                elif line.find("IRRIGATION") >= 0:
                    fout.write(line)
                    fout.write(blines[l + 1])
                    toks = blines[l + 2].split()
                    olddate = toks[0]
                    amount = toks[2]
                    if irrigation is None:
                        it = "{0:04d}-{1:02d}-{2:02d}".format(
                            self.startyear, self.startmonth, self.startday)
                        irrigation = {it: 0.0}
                    for i in irrigation.keys():
                        dt = date(*map(int, i.split("-")))
                        try:
                            assert dt >= date(self.startyear, self.startmonth, self.startday) and dt <= date(
                                self.endyear, self.endmonth, self.endday)
                        except:
                            print(
                                "Irrigation date selected outside simulation period.")
                            sys.exit()
                        dts = "{0:04d}{1}".format(dt.year, dt.strftime("%j"))
                        fout.write(
                            blines[l + 2].replace(olddate, dts).replace(amount, str(irrigation[i])))
                    l += 3
                elif line.find("FERTILIZERS") >= 0:
                    fout.write(line)
                    toks = blines[l + 1].split()
                    olddate = toks[0]
                    amount = toks[3]
                    percent = toks[4]
                    if fertilizers is None:
                        if planting is not None:
                            fertilizers = {(planting + timedelta(10)).strftime("%Y-%m-%d"): [
                                amount, percent], (planting + timedelta(40)).strftime("%Y-%m-%d"): [amount, percent]}
                    for f in fertilizers.keys():
                        dt = date(*map(int, f.split("-")))
                        try:
                            assert dt >= date(self.startyear, self.startmonth, self.startday) and dt <= date(
                                self.endyear, self.endmonth, self.endday)
                        except:
                            print(
                                "Fertilization date selected outside simulation period.")
                            sys.exit()
                        dts = "{0:04d}{1}".format(dt.year, dt.strftime("%j"))
                        fout.write(blines[l + 1].replace(olddate, dts).replace(
                            amount, str(fertilizers[f][0])).replace(percent, str(fertilizers[f][1])))
                    l += 2
                else:
                    fout.write(line)
                    l += 1
            fout.close()
        return dz, smi

    def _planting(self, lat, lon, crop="maize"):
        """Retrieve planting dates for pixel."""
        db = dbio.connect(self.dbname)
        cur = db.cursor()
        sql = "select st_value(rast,st_geomfromtext('POINT({0} {1})',4326)) as doy from crops.plantstart where type like '{2}' and st_intersects(rast,st_geomfromtext('POINT({0} {1})',4326)) order by doy".format(
            lon, lat, crop)
        cur.execute(sql)
        results = cur.fetchall()
        dt = [date(self.startyear, 1, 1) + timedelta(r[0] - 1)
              for r in results if r[0] is not None]
        cur.close()
        db.close()
        return dt

    def _cultivar(self, ens, gid):  # lat, lon):
        """Retrieve Cultivar parameters for pixel and ensemble member."""
        db = dbio.connect(self.dbname)
        cur = db.cursor()
        sql = "select p1,p2,p5,g2,g3,phint from dssat.cultivars as c,{0}.agareas as a where ensemble={1} and st_intersects(c.geom,a.geom) and a.gid={2}".format(
            self.name, ens + 1, gid)
        cur.execute(sql)
        if bool(cur.rowcount):
            sql = "select p1,p2,p5,g2,g3,phint from dssat.cultivars as c,{0}.agareas as a where ensemble={1} and a.gid={2} order by st_centroid(c.geom) <-> st_centroid(a.geom)".format(
                self.name, ens + 1, gid)
            cur.execute(sql)
        p1, p2, p5, g2, g3, phint = cur.fetchone()
        cultivar = "990002 MEDIUM SEASON    IB0001  {0:.1f} {1:.3f} {2:.1f} {3:.1f}  {4:.2f} {5:.2f}".format(
            p1, p2, p5, g2, g3, phint)
        cur.close()
        db.close()
        return cultivar

    def _interpolateSoilMoist(self, sm, depths, dz):
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

    def writeConfigFile(self, modelpath, nlayers, startdate, enddate):
        """Write DSSAT-ENKF config file."""
        fout = open("{0}/ENKF_CONFIG.TXT".format(modelpath), 'w')
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

    def save(self, modelpaths, startdt):
        """Saves DSSAT output to database."""
        db = dbio.connect(self.dbname)
        cur = db.cursor()
        cur.execute(
            "select * from information_schema.tables where table_name='dssat' and table_schema='{0}'".format(self.name))
        if bool(cur.rowcount):
            cur.execute("drop table {0}.dssat".format(self.name))
        cur.execute("create table {0}.dssat (id serial primary key, gid int, ensemble int, fdate date, wsgd real, lai real, gwad real, geom geometry, CONSTRAINT enforce_dims_geom CHECK (st_ndims(geom) = 2), CONSTRAINT enforce_geotype_geom CHECK (geometrytype(geom) = 'POLYGON'::text OR geometrytype(geom) = 'MULTIPOLYGON'::text OR geom IS NULL))".format(self.name))
        db.commit()
        sql = "insert into {0}.dssat (fdate, gid, ensemble, gwad, wsgd, lai) values (%(dt)s, %(gid)s, %(ens)s, %(gwad)s, %(wsgd)s, %(lai)s)".format(
            self.name)
        for gid, pi in modelpaths:
            modelpath = modelpaths[(gid, pi)]
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
                        if float(data[9]) > 0.0:
                            cur.execute(sql, {'dt': dts, 'ens': e + 1, 'gwad': float(
                                data[9]), 'wsgd': float(data[18]), 'lai': float(data[6]), 'gid': gid})
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

    def _readShapefile(self):
        """Read areas from shapefile where DSSAT will be run."""
        try:
            cmd = "{0}/shp2pgsql -s 4326 -d -I -g geom {1} {2}.agareas | {0}/psql -d {3}".format(rpath.bins, self.shapefile, self.name, self.dbname)
            subprocess.call(cmd, shell=True)
            db = dbio.connect(self.dbname)
            cur = db.cursor()
            sql = "select gid, st_x(st_centroid(geom)), st_y(st_centroid(geom)) from {0}.agareas".format(self.name)
            cur.execute(sql)
            geoms = cur.fetchall()
            return geoms
        except:
            print("Shapefile {0} for DSSAT simulation does not exist. Exiting...".format(
                self.shapefile))
            sys.exit()

    def run(self, dssatexe, crop_threshold=0.1):
        """Runs DSSAT simulation."""
        exe = dssatexe.split("/")[-1]
        startdt = date(self.startyear, self.startmonth, self.startday)
        self.readVICSoil()
        modelpaths = OrderedDict()
        pwd = os.getcwd()
        geoms = self._readShapefile()
        cropfract = self._calcCroplandFract()
        simstartdt = None
        for geom in geoms:
            gid, lon, lat = geom
            c = np.argmin(np.sqrt((lat - self.lat) **
                                  2 + (lon - self.lon) ** 2))
            # use the soil depths from the nearest VIC pixel to the centroid
            depths = np.array(self.depths[c])
            if cropfract[gid] >= crop_threshold:
                year, month, day, weather, sm, vlai = self.readVICOutput(
                    gid, depths)
                vicstartdt = date(year[0], month[0], day[0])
                plantdates = self._planting(lat, lon)
                planting = [p for p in plantdates if p >= startdt and p <= date(
                    self.endyear, self.endmonth, self.endday)]
                if planting is []:
                    planting = [plantdates[
                        np.argmax([(t - startdt).days for t in plantdates if (t - startdt).days < 0])]]
                for pi, pdt in enumerate(planting[:1]):
                    modelpath = os.path.abspath(
                        "{0}/{1}_{2}_{3}".format(self.path, lat, lon, pi))
                    modelpaths[(gid, pi)] = modelpath
                    os.mkdir(modelpath)
                    os.mkdir(modelpath + "/ENKF_Results")
                    shutil.copy(dssatexe, modelpath)
                    distutils.dir_util.copy_tree(
                        "{0}/dssat".format(rpath.data), modelpath)
                    if pdt > date(pdt.year, 1, 8):
                        simstartdt = pdt - timedelta(7)
                    else:
                        simstartdt = pdt
                    dz, smi = self.writeControlFile(modelpath, sm, depths, simstartdt, gid, self.lat[
                                                    c], self.lon[c], pdt, None, None)
                    if simstartdt < vicstartdt:
                        print("No input data for DSSAT corresponding to starting date {0}. Need to run VIC for these dates. Exiting...".format(
                            simstartdt.strftime('%Y-%m-%d')))
                        sys.exit()
                    ti0 = [i for i in range(len(year)) if simstartdt == date(
                        year[i], month[i], day[i])][0]
                    if pi + 1 < len(planting):
                        ti1 = [i for i in range(len(year)) if (
                            planting[pi + 1] - timedelta(10)) == date(year[i], month[i], day[i])][0]
                    else:
                        ti1 = [i for i in range(len(year)) if (planting[pi] + timedelta(min(180, len(year) - (planting[pi] - date(
                            self.startyear - 1, 12, 31)).days))) == date(year[i], month[i], day[i])][0]  # len(year) - 1
                    self.writeWeatherFiles(modelpath, self.name, year, month, day, weather, self.elev[
                                           c], self.lat[c], self.lon[c], ti0, ti1)
                    self.writeSoilMoist(modelpath, year, month, day, smi, dz)
                    self.writeLAI(modelpath, gid, viclai=vlai)
                    self.writeConfigFile(modelpath, smi.shape[1], simstartdt, date(
                        year[ti1], month[ti1], day[ti1]))
                    print("Wrote DSSAT for planting date {0}".format(
                        pdt.strftime("%Y-%m-%d")))
                    os.chdir(pwd)
        p = multiprocessing.Pool(multiprocessing.cpu_count())
        for modelpath in modelpaths.values():
            p.apply_async(_run1, (modelpath, exe, self.assimilate))
        p.close()
        p.join()
        os.chdir(pwd)
        if simstartdt:
            self.save(modelpaths, simstartdt)
        else:
            print("WARNING! No crop areas found!")
        shutil.rmtree(self.path)
