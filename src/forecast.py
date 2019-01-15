""" RHEAS module for forecasting

.. module:: forecast
   :synopsis: Definition of the forecast module

.. moduleauthor:: Kostas Andreadis <kandread@jpl.nasa.gov>

"""

import ensemble
import config
import shutil
import sys
from assimilation import assimilate
from datetime import date
from dateutil.relativedelta import relativedelta
import dbio
import vic
import dssat
import rpath
import raster
import logging


def runVIC(dbname, options):
    """Driver function for performing a VIC forecast simulation"""
    log = logging.getLogger(__name__)
    startyear, startmonth, startday = map(
        int, options['forecast']['startdate'].split('-'))
    endyear, endmonth, endday = map(
        int, options['forecast']['enddate'].split('-'))
    if date(endyear, endmonth, endday) > (date(startyear, startmonth, startday) + relativedelta(months=6)):
        log.warning("Forecast with lead time longer than 6 months requested. Exiting...")
        sys.exit()
    res = config.getResolution(options['forecast'])
    vicexe = "{0}/vicNl".format(rpath.bins)
    basin = config.getBasinFile(options['forecast'])
    saveto, savevars = config.getVICvariables(options)
    name = options['forecast']['name'].lower()
    nens = int(options['forecast']['ensemble size'])
    method = options['forecast']['method']
    name = options['forecast']['name'].lower()
    models = ensemble.Ensemble(nens, dbname, res, startyear,
                               startmonth, startday, endyear, endmonth, endday, name)
    if 'initialize' in options['vic']:
        init = options['vic']['initialize']
    else:
        init = True  # default is to always initialize forecast
    if init:
        statefile = initializeVIC(models[0], basin, res, options['vic'], vicexe)
    else:
        statefile = None
    models.writeParamFiles(statefile=statefile)
    models.writeSoilFiles(basin)
    models.writeForcings(method, options)
    models.run(vicexe)
    models.setDates(startyear, startmonth, startday, endyear, endmonth, endday)
    models.save(saveto, savevars)
    # for varname in savevars:
    #     raster.stddev(models.dbname, "{0}.{1}".format(
    #         models.name, varname))
    #     raster.mean(models.dbname, "{0}.{1}".format(
    #         models.name, varname))
    # for e in range(nens):
    #     shutil.rmtree(models[e].model_path)


def initializeVIC(model, basin, res, voptions, vicexe):
    """Initialize a VIC forecast by:
    1. Checking for existing state file
    2. Running a deterministic simulation (at most 1-year long)."""
    statefile = model.stateFile()
    if statefile is None:
        db = dbio.connect(model.dbname)
        cur = db.cursor()
        if dbio.tableExists(model.dbname, model.name, "state"):
            cur.execute("select max(fdate) from {0}.state where fdate<=date'{1}-{2}-{3}'".format(model.name, model.startyear, model.startmonth, model.startday))
            ts = cur.fetchone()[0]
        else:
            ts = None
        t1 = date(model.startyear-1, model.startmonth, model.startday)
        cur.execute("select min(fdate) from precip.{0}".format(voptions['precip']))
        tp = cur.fetchone()[0]
        cur.execute("select min(fdate) from tmax.{0}".format(voptions['temperature']))
        tx = cur.fetchone()[0]
        cur.execute("select min(fdate) from wind.{0}".format(voptions['wind']))
        tw = cur.fetchone()[0]
        tm = max(tp, tx, tw)  # earliest date where meteorological data are available
        t0 = max(t1, tm) if ts is None else max(ts, t1, tm)  # earliest date where meteorological data or statefile is available (within 1 year)
        init = (t0 == ts)  # initialize from statefile if criteria are met
        model = vic.VIC(model.model_path, model.dbname, res, t0.year, t0.month, t0.day, model.startyear, model.startmonth, model.startday, model.name)
        model.writeParamFile(save_state=True, init_state=init)
        model.writeSoilFile(basin)
        prec, tmax, tmin, wind = model.getForcings(voptions)
        model.writeForcings(prec, tmax, tmin, wind)
        model.run(vicexe)
        statefile = "{0}/{1}".format(model.model_path, model.statefile)
    return statefile


def runDSSAT(dbname, options):
    """Driver function for performing a DSSAT forecast simulation"""
    startyear, startmonth, startday = map(
        int, options['forecast']['startdate'].split('-'))
    endyear, endmonth, endday = map(
        int, options['forecast']['enddate'].split('-'))
    res = float(options['forecast']['resolution'])
    nens = int(options['dssat']['ensemble size'])
    name = options['forecast']['name'].lower()
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
    model.run()


def execute(dbname, options):
    """Driver routine for a forecast simulation."""
    log = logging.getLogger(__name__)
    forecast_options = options['forecast']
    if 'model' in forecast_options:
        if 'vic' in forecast_options['model']:
            if 'vic' in options:
                runVIC(dbname, options)
            else:
                log.error("No configuration options for VIC model.")
                sys.exit()
        if 'dssat' in forecast_options['model']:
            if 'dssat' in options:
                runDSSAT(dbname, options)
            else:
                log.error("No configuration options for DSSAT model.")
                sys.exit()
    else:
        log.error("No model selected for forecast.")
        sys.exit()
