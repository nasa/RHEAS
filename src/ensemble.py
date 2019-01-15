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
import logging


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

    def __getitem__(self, m):
        """Return a model instance."""
        return self.models[m]

    def __len__(self):
        """Return ensemble size."""
        return len(self.models)

    def __iter__(self):
        """Return an iterator to model ensemble members."""
        return iter(self.models)

    def writeParamFiles(self, savestate=False, statefile=""):
        """Write model parameter file for each ensemble member."""
        for e, model in enumerate(self.models):
            if len(self.statefiles) > 0:
                model.writeParamFile(state_file=self.statefiles[e], save_state=savestate)
            else:
                model.writeParamFile(state_file=statefile, save_state=savestate)

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
        log = logging.getLogger(__name__)
        if method.lower() == "esp":
            self._ESP(options)
        elif method.lower() == "nmme":
            self.__fromDataset("nmme", options)
        else:
            log.error("No appropriate method for generating meteorological forecast ensemble, exiting!")
            sys.exit()

    def __fromDataset(self, dataset, options):
        """Generate and write forcings by using a dataset-specific function."""
        dsmod = __import__("datasets." + dataset, fromlist=[dataset])
        dsmod.generate(options, self)

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
        years.remove(self.endyear)  # remove forecast end year to ensure adequate record length in resampled dataset
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

    def _ensembleTable(self, write, e):
        def write_wrapper(data, dates, tablename, initialize, skipsave):
            return write(data, dates, tablename, initialize, e, skipsave=skipsave)
        return write_wrapper

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
                model.save(saveto, args, initialize, ensemble=e)
            else:
                if e < 1:
                    if os.path.isdir(saveto):
                        shutil.rmtree(saveto)
                    elif os.path.isfile(saveto):
                        os.remove(saveto)
                    os.makedirs(saveto)
                model.save(saveto + "/{0}".format(e + 1), args, False)

    def run(self, vicexe):
        """Run ensemble of VIC models using multi-threading."""
        procs = [Process(target=self.models[e].run, args=(vicexe,))
                 for e in range(self.nens)]
        for p in procs:
            p.start()
        for p in procs:
            p.join()
