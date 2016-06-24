""" Class definition for the ensemble interface

.. module:: ensemble
   :synopsis: Definition of the ensemble class

.. moduleauthor:: Kostas Andreadis <kandread@jpl.nasa.gov>

"""

import vic
from vic import state
import tempfile
import sys
import random
from datetime import date, timedelta
from multiprocessing import Process
import numpy as np
import shutil
import os
from dateutil.relativedelta import relativedelta
import rpath
import dbio


class Ensemble:

    def __init__(self, nens, dbname, resolution, startyear, startmonth, startday,
                 endyear, endmonth, endday, name=""):
        """Create an ensemble of models with size *nens*."""
        self.nens = nens
        self.models = []
        self.name = name
        self.statefiles = []
        self.res = resolution
        self.startyear, self.startmonth, self.startday = startyear, startmonth, startday
        self.endyear, self.endmonth, self.endday = endyear, endmonth, endday
        self.dbname = dbname
        for e in range(nens):
            modelpath = tempfile.mkdtemp(dir=".")
            model = vic.VIC(modelpath, dbname, resolution, startyear, startmonth, startday,
                            endyear, endmonth, endday, name=name)
            self.models.append(model)

    def _ensembleTable(self, write, e):
        def write_wrapper(data, dates, tablename, initialize, skipsave):
            return write(data, dates, tablename, initialize, e, skipsave=skipsave)
        return write_wrapper

    def setStateFiles(self, statefiles):
        """Set initial state files for each ensemble member."""
        for e in range(len(statefiles)):
            try:
                shutil.copy(statefiles[e], self.models[e].model_path)
            except:
                pass
            filename = statefiles[e].split("/")[-1]
            statefiles[
                e] = "{0}/{1}".format(self.models[e].model_path, filename)
        self.statefiles = statefiles

    def readStateFiles(self):
        """Read initial state files for each ensemble member."""
        cells = []
        _, vegfile, snowbandfile = self.models[0].paramFromDB()
        veg = state.readVegetation("{0}/{1}".format(rpath.data, vegfile))
        bands, _ = state.readSnowbands(
            "{0}/{1}".format(rpath.data, snowbandfile))
        for filename in self.statefiles:
            c, _, _, _ = state.readStateFile(filename)
            cells.append(c)
        return cells, veg, bands

    def updateStateFiles(self, data, alat, alon, agid):
        """Update initial state files with *data*."""
        _, vegparam, snowbands = self.models[0].paramFromDB()
        veg = state.readVegetation("{0}/{1}".format(rpath.data, vegparam))
        bands, _ = state.readSnowbands("{0}/{1}".format(rpath.data, snowbands))
        for e, statefile in enumerate(self.statefiles):
            states, nlayer, nnodes, dateline = state.readStateFile(statefile)
            for var in data:
                x = state.readVariable(self.models[e], states, alat[var], alon[
                                       var], veg, bands, nlayer, var)

                states = state.updateVariable(self.models[e], states, x, data[var][:, e], alat[
                                              var], alon[var], agid, veg, bands, nlayer, var)
            state.writeStateFile(statefile, states, "{0}\n{1} {2}".format(
                dateline.strip(), nlayer, nnodes))

    def setDates(self, startyear, startmonth, startday, endyear, endmonth, endday):
        """Set simulation dates for entire ensemble."""
        self.startyear, self.startmonth, self.startday = startyear, startmonth, startday
        self.endyear, self.endmonth, self.endday = endyear, endmonth, endday
        for m in self.models:
            m.startyear = startyear
            m.startmonth = startmonth
            m.startday = startday
            m.endyear = endyear
            m.endmonth = endmonth
            m.endday = endday

    def __getitem__(self, m):
        """Return a model instance."""
        return self.models[m]

    def __len__(self):
        """Return ensemble size."""
        return len(self.models)

    def __iter__(self):
        """Return an iterator to model ensemble members."""
        return iter(self.models)

    def writeParamFiles(self, savestate=""):
        """Write model parameter file for each ensemble member."""
        for e, model in enumerate(self.models):
            if len(self.statefiles) > 0:
                model.writeParamFile(state_file=self.statefiles[
                                     e], save_state=savestate)
            else:
                model.writeParamFile(save_state=savestate)

    def writeSoilFiles(self, shapefile):
        """Write soil parameter files based on domain shapefile."""
        self.models[0].writeSoilFile(shapefile)
        for model in self.models[1:]:
            shutil.copy(
                "{0}/soil.txt".format(self.models[0].model_path), "{0}/".format(model.model_path))
            model.lat = self.models[0].lat
            model.lon = self.models[0].lon
            model.gid = self.models[0].gid
            model.lgid = self.models[0].lgid
            model.depths = self.models[0].depths
            model.elev = self.models[0].elev

    def writeForcings(self, method, options):
        """Write forcings for the ensemble based on method (ESP, BCSD)."""
        if method.lower() == "esp":
            self._ESP(options)
        elif method.lower() == "bcsd":
            pass
        elif method.lower() == "iri":
            self.__fromDataset("iri", options)
        else:
            print(
                "ERROR! No appropriate method for generating meteorological forecast ensemble, exiting!")
            sys.exit()

    def __fromDataset(self, dataset, options):
        """Generate and write forcings by using a dataset-specific function."""
        dsmod = __import__("datasets." + dataset, fromlist=[dataset])
        dsmod.generate(options, self)

    def perturb(self, prec, tmax, tmin, wind, nens=None, perr=0.25, terr=2.0):
        """Perturb meteorological forcings."""
        if nens is None:
            nens = self.nens
        ensprec = []
        enstmax = []
        enstmin = []
        enswind = []
        for e in range(nens):
            p = []
            tx = []
            tn = []
            w = []
            for i in range(len(prec)):
                p.append(list(prec[i]))
                if prec[i][2] > 0.0:
                    p[-1][2] = np.log(np.random.lognormal(prec[i]
                                                          [2], abs(perr * prec[i][2])))
                tx.append(list(tmax[i]))
                tn.append(list(tmin[i]))
                tavgp = 0.5 * (tmax[i][2] + tmin[i][2]) + \
                    np.random.normal(0., terr)
                tx[-1][2] = (tavgp - 0.5 * tmin[i][2]) / 0.5
                tn[-1][2] = (tavgp - 0.5 * tmax[i][2]) / 0.5
                w.append(list(wind[i]))
            ensprec.append(p)
            enstmax.append(tx)
            enstmin.append(tn)
            enswind.append(w)
        return ensprec, enstmax, enstmin, enswind

    def _ESP(self, options):
        """Generate meteorological forcings using the Ensemble Streamflow Prediction method."""
        ndays = (date(self.endyear, self.endmonth, self.endday) -
                 date(self.startyear, self.startmonth, self.startday)).days
        db = dbio.connect(self.models[0].dbname)
        cur = db.cursor()
        if self.startmonth < self.endmonth:
            sql = "select distinct (date_part('year', fdate)) as year from precip.{0} where date_part('month', fdate) >= {1} and date_part('month', fdate) <= {2}".format(options['vic']['precip'], self.startmonth, self.endmonth)
        else:
            sql = "select distinct (date_part('year', fdate)) as year from precip.{0} where date_part('month', fdate) >= {1} or date_part('month', fdate) <= {2}".format(options['vic']['precip'], self.startmonth, self.endmonth)
        cur.execute(sql)
        years = map(lambda y: int(y[0]), cur.fetchall())
        random.shuffle(years)
        while len(years) < self.nens:
            years += years
        for e in range(self.nens):
            model = self.models[e]
            model.startyear = years[e]
            t = date(model.startyear, model.startmonth,
                     model.startday) + timedelta(ndays)
            model.endyear, model.endmonth, model.endday = t.year, t.month, t.day
            prec, tmax, tmin, wind = model.getForcings(options['vic'])
            model.writeForcings(prec, tmax, tmin, wind)
        cur.close()
        db.close()

    def run(self, vicexe):
        """Run ensemble of VIC models using multi-threading."""
        procs = [Process(target=self.models[e].run, args=(vicexe,))
                 for e in range(self.nens)]
        for p in procs:
            p.start()
        for p in procs:
            p.join()

    def _initializeDeterm(self, basin, forcings, vicexe):
        """Initialize ensemble of VIC models deterministically."""
        db = dbio.connect(self.dbname)
        cur = db.cursor()
        dt = "{0}-{1}-{2}".format(self.startyear,
                                  self.startmonth, self.startday)
        cur.execute(
            "select * from information_schema.tables where table_schema='{0}' and table_name='state'".format(self.name))
        if bool(cur.rowcount):
            sql = "select filename, fdate from {0}.state order by abs(date '{1}' - fdate)".format(
                self.name, dt)
            cur.execute(sql)
            if bool(cur.rowcount):
                statefile, t = cur.fetchone()
            else:
                statefile = ""
        else:
            statefile = ""
        if statefile == "":
            t = date(self.startyear - 1, self.startmonth, self.startday)
        # checks if statefile corresponds to requested forecast start date
        if (t - date(self.startyear, self.startmonth, self.startday)).days < 0:
            if (t - date(self.startyear - 1, self.startmonth, self.startday)).days < 0:
                # if statefile is older than a year, start the model
                # uninitialized for 1 year
                t = date(self.startyear - 1, self.startmonth, self.startday)
            modelpath = tempfile.mkdtemp(dir=".")
            model = vic.VIC(modelpath, self.dbname, self.res, t.year, t.month,
                            t.day, self.startyear, self.startmonth, self.startday, self.name)
            model.writeParamFile(save_state=modelpath,
                                 init_state=bool(statefile))
            model.writeSoilFile(basin)
            prec, tmax, tmin, wind = model.getForcings(forcings)
            model.writeForcings(prec, tmax, tmin, wind)
            model.run(vicexe)
            statefile = model.model_path + \
                "/vic.state_{0:04d}{1:02d}{2:02d}".format(
                    self.startyear, self.startmonth, self.startday)
            for emodel in self.models:
                shutil.copy(statefile, emodel.model_path)
            shutil.rmtree(model.model_path)
        statefiles = [statefile] * self.nens
        self.setStateFiles(statefiles)
        cur.close()
        db.close()
        return statefiles

    def _initializeRandom(self, basin, forcings, vicexe, initdays=90, saveindb=False, saveto="db", saveargs=[], overwrite=True, skipsave=0):
        """Initialize ensemble of VIC models by sampling the meterological forcings
        and running them *initmonths* prior to simulation start date."""
        db = dbio.connect(self.dbname)
        cur = db.cursor()
        sql = "select distinct (date_part('year', fdate)) as year from precip.{0}".format(
            forcings['precip'])
        cur.execute(sql)
        years = map(lambda y: int(y[0]), cur.fetchall())
        years.remove(max(years))
        cur.close()
        db.close()
        statefiles = []
        t = date(self.startyear, self.startmonth, self.startday) - \
            relativedelta(days=initdays)
        ndays = (date(self.startyear, self.startmonth, self.startday) - t).days
        modelpath = tempfile.mkdtemp()  # (dir=".")
        model = vic.VIC(modelpath, self.dbname, self.res, t.year, t.month,
                        t.day, self.startyear, self.startmonth, self.startday, self.name)
        years = np.random.choice(years, self.nens)
        pmodels = []
        for e in range(self.nens):
            modelpath = tempfile.mkdtemp()  # (dir=".")
            model = vic.VIC(modelpath, self.dbname, self.res, t.year, t.month,
                            t.day, self.startyear, self.startmonth, self.startday, self.name)
            model.writeParamFile(save_state=modelpath, init_state=False)
            model.writeSoilFile(basin)
            model.startyear = years[e]
            model.endyear = years[e] + (model.endyear - t.year)
            ddays = (date(model.endyear, model.endmonth, model.endday) -
                     date(model.startyear, model.startmonth, model.startday)).days - ndays
            t1 = date(model.endyear, model.endmonth, model.endday) - \
                relativedelta(days=ddays)
            model.endyear, model.endmonth, model.endday = t1.year, t1.month, t1.day
            prec, tmax, tmin, wind = model.getForcings(forcings)
            model.writeForcings(prec, tmax, tmin, wind)
            model.startyear, model.startmonth, model.startday = t.year, t.month, t.day
            model.endyear, model.endmonth, model.endday = self.startyear, self.startmonth, self.startday
            pmodels.append(model)
        procs = [Process(target=pmodels[e].run, args=(vicexe,))
                 for e in range(self.nens)]
        for p in procs:
            p.start()
        for p in procs:
            p.join()
        if saveindb:
            if skipsave < 0:
                skipdays = (date(self.startyear, self.startmonth,
                                 self.startday) - t).days + 1 + skipsave
            else:
                skipdays = skipsave
            init = overwrite
            for e in range(len(pmodels)):
                model = pmodels[e]
                if getattr(model.writeToDB, 'func_name') is not 'write_wrapper':
                    model.writeToDB = self._ensembleTable(
                        model.writeToDB, e + 1)
                if e > 0:
                    init = False
                model.save(saveto, saveargs, init, skipsave=skipdays)
        for e in range(self.nens):
            statefile = pmodels[e].model_path + "/vic.state_{0:04d}{1:02d}{2:02d}".format(
                self.startyear, self.startmonth, self.startday)
            statefiles.append(statefile)
        return statefiles

    def _initializePerturb(self, basin, forcings, vicexe, initdays=90, saveindb=False, saveto="db", saveargs=[], overwrite=True, skipsave=0):
        """Initialize ensemble of VIC models by perturbing the meterological forcings
        and running them *initmonths* prior to simulation start date."""
        statefiles = []
        t = date(self.startyear, self.startmonth, self.startday) - \
            relativedelta(days=initdays)
        modelpath = tempfile.mkdtemp()
        model = vic.VIC(modelpath, self.dbname, self.res, t.year, t.month,
                        t.day, self.startyear, self.startmonth, self.startday, self.name)
        prec, tmax, tmin, wind = model.getForcings(forcings)
        eprec, etmax, etmin, ewind = self.perturb(prec, tmax, tmin, wind)
        pmodels = []
        for e in range(self.nens):
            modelpath = tempfile.mkdtemp()
            model = vic.VIC(modelpath, self.dbname, self.res, t.year, t.month,
                            t.day, self.startyear, self.startmonth, self.startday, self.name)
            model.writeParamFile(save_state=modelpath, init_state=False)
            model.writeSoilFile(basin)
            model.writeForcings(eprec[e], etmax[e], etmin[e], ewind[e])
            pmodels.append(model)
        procs = [Process(target=pmodels[e].run, args=(vicexe,))
                 for e in range(self.nens)]
        for p in procs:
            p.start()
        for p in procs:
            p.join()
        if saveindb:
            if skipsave < 0:
                skipdays = (date(self.startyear, self.startmonth,
                                 self.startday) - t).days + 1 + skipsave
            else:
                skipdays = skipsave
            init = overwrite
            for e in range(len(pmodels)):
                model = pmodels[e]
                if getattr(model.writeToDB, 'func_name') is not 'write_wrapper':
                    model.writeToDB = self._ensembleTable(
                        model.writeToDB, e + 1)
                if e > 0:
                    init = False
                model.save(saveto, saveargs, init, skipsave=skipdays)
        for e in range(self.nens):
            statefile = pmodels[e].model_path + "/vic.state_{0:04d}{1:02d}{2:02d}".format(
                self.startyear, self.startmonth, self.startday)
            statefiles.append(statefile)
        return statefiles

    def initialize(self, options, basin, method, vicexe, saveindb=False, saveto="db", saveargs=[], overwrite=True, skipsave=0, initdays=90):
        """Initialize ensemble of VIC models using one of three methods:
        1) deterministic (default): each ensemble member has an identical state
        2) random: each ensemble member gets a random day from climatology
        3) perturb: perturb precipitation and temperature"""
        forcings = {'temperature': options['vic'][
            'temperature'], 'wind': options['vic']['wind']}
        if 'lai' in options['vic']:
            forcings['lai'] = options['vic']['lai']
        forcings['precip'] = options['vic']['precip'].split(",")[0]
        self.writeParamFiles()
        # write soil file for each ensemble member and populate
        # latitude/longitude arrays
        self.writeSoilFiles(basin)
        if method.find("determ") == 0:
            statefiles = self._initializeDeterm(basin, forcings, vicexe)
        elif method.find("states") == 0:
            db = dbio.connect(self.dbname)
            cur = db.cursor()
            cur.execute(
                "select * from information_schema.tables where table_name='state' and table_schema=%s", (self.name,))
            if bool(cur.rowcount):
                cur.execute("select filename from {0}.state where date_part('month', fdate) = {1}".format(
                    self.name, self.startmonth))
                statefiles = map(lambda q: q[0], cur.fetchall())
                statefiles = list(np.random.choice(statefiles, self.nens))
            else:
                print(
                    "WARNING! No statefiles found in the database. Not initializing ensemble!")
                statefiles = []
            cur.close()
            db.close()
        elif method.find("random") == 0:
            statefiles = self._initializeRandom(
                basin, forcings, vicexe, initdays=initdays, saveindb=saveindb, saveto=saveto, saveargs=saveargs, skipsave=skipsave, overwrite=overwrite)
        elif method.find("perturb") == 0:
            statefiles = self._initializePerturb(
                basin, forcings, vicexe, initdays=initdays, saveindb=saveindb, saveto=saveto, saveargs=saveargs, skipsave=skipsave, overwrite=overwrite)
        else:
            print("No appropriate method to initialize the ensemble found!")
            sys.exit()
        self.setStateFiles(statefiles)

    def save(self, saveto, args, initialize=True):
        """Reads and saves selected output data variables from the ensemble into the database
        or a user-defined directory."""
        def ensembleTable(write, e):
            def write_wrapper(data, dates, tablename, initialize, skipsave):
                return write(data, dates, tablename, initialize, e)
            return write_wrapper
        for e in range(self.nens):
            model = self.models[e]
            if getattr(model.writeToDB, 'func_name') is not 'write_wrapper':
                # decorate function to add ensemble information
                model.writeToDB = ensembleTable(model.writeToDB, e + 1)
            if saveto == "db":
                if e > 0:
                    initialize = False
                model.save(saveto, args, initialize)
            else:
                if e < 1:
                    if os.path.isdir(saveto):
                        shutil.rmtree(saveto)
                    elif os.path.isfile(saveto):
                        os.remove(saveto)
                    os.makedirs(saveto)
                model.save(saveto + "/{0}".format(e + 1), args, False)
