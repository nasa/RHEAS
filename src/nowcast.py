""" RHEAS module for nowcast simulations.

.. module:: nowcast
   :synopsis: Module that contains functionality for nowcast simulations

.. moduleauthor:: Kostas Andreadis <kandread@jpl.nasa.gov>

"""

import vic
import config
import ensemble
import dssat
import sys
import tempfile
import shutil
from assimilation import assimilate, observationDates
from datetime import date, timedelta
import rpath
import raster
import dbio


def runVIC(dbname, options):
    """Driver function for performing a VIC nowcast simulation"""
    if any(opt in options['vic'] for opt in ['ensemble size', 'observations']) or len(options['vic']['precip'].split(",")) > 1:
        runEnsembleVIC(dbname, options)
    else:
        runDeterministicVIC(dbname, options)


def _saveState(vicoptions):
    """Should VIC state file be saved?"""
    if 'save state' in vicoptions:
        savestate = vicoptions['save state']
        dbsavestate = True
    else:
        savestate = ""
        dbsavestate = False
    return savestate, dbsavestate


def _initialize(vicoptions):
    """Should VIC be initialized from a model state file?"""
    if 'initialize' in vicoptions:
        init = vicoptions['initialize']
    else:
        init = False
    if 'initial state' in vicoptions:
        statefile = vicoptions['initial state']
    else:
        statefile = ""
    return init, statefile


def runDeterministicVIC(dbname, options):
    """Driver function for performing a deterministic VIC nowcast simulation."""
    res = config.getResolution(options['nowcast'])
    vicexe = "{0}/vicNl".format(rpath.bins)
    basin = config.getBasinFile(options['nowcast'])
    saveto, savevars = config.getVICvariables(options)
    startyear, startmonth, startday = map(
        int, options['nowcast']['startdate'].split('-'))
    endyear, endmonth, endday = map(
        int, options['nowcast']['enddate'].split('-'))
    name = options['nowcast']['name'].lower()
    path = tempfile.mkdtemp(dir=".")
    model = vic.VIC(path, dbname, res, startyear, startmonth,
                    startday, endyear, endmonth, endday, name)
    savestate, dbsavestate = _saveState(options['vic'])
    init, statefile = _initialize(options['vic'])
    model.writeParamFile(save_state=savestate, init_state=init,
                         save_state_to_db=dbsavestate, state_file=statefile)
    model.writeSoilFile(basin)
    prec, tmax, tmin, wind = model.getForcings(options['vic'])
    model.writeForcings(prec, tmax, tmin, wind)
    model.run(vicexe)
    model.save(saveto, savevars)
    shutil.rmtree(path)


def runEnsembleVIC(dbname, options):
    """Driver function for performing a VIC nowcast simulation."""
    res = config.getResolution(options['nowcast'])
    name = options['nowcast']['name'].lower()
    vicexe = "{0}/vicNl".format(rpath.bins)
    basin = config.getBasinFile(options['nowcast'])
    saveto, savevars = config.getVICvariables(options)
    startyear, startmonth, startday = map(
        int, options['nowcast']['startdate'].split('-'))
    endyear, endmonth, endday = map(
        int, options['nowcast']['enddate'].split('-'))
    precipdatasets = options['vic']['precip'].split(",")
    savestate, _ = _saveState(options['vic'])
    if 'ensemble size' in options['vic']:
        nens = int(options['vic']['ensemble size'])
    elif 'observations' in options['vic']:
        nens = 20
    else:
        nens = len(precipdatasets)
    models = ensemble.Ensemble(nens, dbname, res, startyear,
                               startmonth, startday, endyear, endmonth, endday, name)
    if 'initialize' in options['vic'] and options['vic']['initialize']:
        init_method = options['vic']['initialize']
        if isinstance(init_method, bool):
            init_method = "determ"
        models.initialize(options, basin, init_method, vicexe)
    else:
        models.writeSoilFiles(basin)
    if 'observations' in options['vic']:
        method = "random"
        obsnames = options['vic']['observations'].split(",")
        if 'update' in options['vic']:
            update = options['vic']['update']
        else:
            update = None
        updateDates = observationDates(
            obsnames, dbname, startyear, startmonth, startday, endyear, endmonth, endday, update)
        t0 = date(startyear, startmonth, startday)
        updateDates += [date(endyear, endmonth, endday)]
        for t in updateDates:
            if t0 == date(startyear, startmonth, startday):
                overwrite = True
            else:
                overwrite = False
            ndays = (date(t.year, t.month, t.day) - t0).days
            t1 = t + timedelta(1)
            models.setDates(t.year, t.month, t.day, t1.year, t1.month, t1.day)
            models.initialize(options, basin, method, vicexe, saveindb=True,
                              saveto=saveto, saveargs=savevars, initdays=ndays, overwrite=overwrite)
            data, alat, alon, agid = assimilate(options, date(
                models.startyear, models.startmonth, models.startday), models)
            db = dbio.connect(models.dbname)
            cur = db.cursor()
            sql = "select tablename from pg_tables where schemaname='{0}'".format(
                models.name)
            cur.execute(sql)
            tables = [tbl[0] for tbl in cur.fetchall() if tbl[0] != "dssat"]
            for tbl in tables:
                sql = "delete from {0}.{1} where fdate=date '{2}-{3}-{4}'".format(
                    models.name, tbl, t.year, t.month, t.day)
            cur.close()
            db.close()
            if bool(data):
                models.updateStateFiles(data, alat, alon, agid)
            t0 = date(t.year, t.month, t.day)
    else:
        method = "random"
        t = date(endyear, endmonth, endday)
        t1 = t + timedelta(1)
        models.setDates(t.year, t.month, t.day, t1.year, t1.month, t1.day)
        ndays = (t - date(startyear, startmonth, startday)).days
        models.initialize(options, basin, method, vicexe, saveindb=True,
                          saveto=saveto, saveargs=savevars, initdays=ndays)
    for varname in savevars:
        raster.stddev(models.dbname, "{0}.{1}".format(
            models.name, varname))
    for model in models:
        shutil.rmtree(model.model_path)


def runDSSAT(dbname, options):
    """Driver function for performing a DSSAT nowcast simulation"""
    startyear, startmonth, startday = map(
        int, options['nowcast']['startdate'].split('-'))
    endyear, endmonth, endday = map(
        int, options['nowcast']['enddate'].split('-'))
    res = float(options['nowcast']['resolution'])
    nens = int(options['dssat']['ensemble size'])
    name = options['nowcast']['name'].lower()
    dssatexe = "{0}/DSSAT_Ex.exe".format(rpath.bins)
    if 'shapefile' in options['dssat']:
        shapefile = options['dssat']['shapefile']
    else:
        shapefile = None
    if 'assimilate' in options['dssat']:
        assimilate = options['dssat']['assimilate']
    else:
        assimilate = "Y"
    model = dssat.DSSAT(dbname, name, res, startyear, startmonth, startday,
                        endyear, endmonth, endday, nens, options['vic'], shapefile, assimilate)
    model.run(dssatexe)


def execute(dbname, options):
    """Driver routine for a nowcast simulation."""
    nowcast_options = options['nowcast']
    if 'model' in nowcast_options:
        if 'vic' in nowcast_options['model']:
            if 'vic' in options:
                runVIC(dbname, options)
            else:
                print "ERROR! No configuration options for VIC model."
                sys.exit()
        if 'dssat' in nowcast_options['model']:
            if 'dssat' in options:
                runDSSAT(dbname, options)
            else:
                print "ERROR! No configuration options for DSSAT model."
                sys.exit()
    else:
        print "ERROR! No model selected for nowcast."
        sys.exit()
