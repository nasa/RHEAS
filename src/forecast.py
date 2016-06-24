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
import dssat
import rpath
import raster


def runVIC(dbname, options):
    """Driver function for performing a VIC forecast simulation"""
    startyear, startmonth, startday = map(
        int, options['forecast']['startdate'].split('-'))
    endyear, endmonth, endday = map(
        int, options['forecast']['enddate'].split('-'))
    # if date(endyear, endmonth, endday) > (date(startyear, startmonth, startday) + relativedelta(months=3)):
    #     print("WARNING! Forecast with lead time longer than 3 months requested. Exiting...")
    #     sys.exit()
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
    if 'initialize' in options['vic'] and options['vic']['initialize'] in ['perturb', 'random']:
        init_method = options['vic']['initialize']
    else:
        init_method = "determ"  # default option to initialize the ensemble from the same state
    # override initializaton method if assimilation was requested
    if 'observations' in options['vic']:
        init_method = "random"
        models.initialize(options, basin, init_method, vicexe,
                          saveindb=True, saveto=saveto, saveargs=savevars, skipsave=-1)
        data, alat, alon, agid = assimilate(options, date(
            models.startyear, models.startmonth, models.startday), models)
        models.updateStateFiles(data, alat, alon, agid)
    else:
        models.initialize(options, basin, init_method, vicexe,
                          saveindb=True, saveto=saveto, saveargs=savevars)
    models.writeParamFiles()
    models.writeForcings(method, options)
    models.run(vicexe)
    models.setDates(startyear, startmonth, startday, endyear, endmonth, endday)
    models.save(saveto, savevars)
    for varname in savevars:
        raster.stddev(models.dbname, "{0}.{1}".format(
            models.name, varname))
    for e in range(nens):
        shutil.rmtree(models[e].model_path)


def runDSSAT(dbname, options):
    """Driver function for performing a DSSAT forecast simulation"""
    startyear, startmonth, startday = map(
        int, options['forecast']['startdate'].split('-'))
    endyear, endmonth, endday = map(
        int, options['forecast']['enddate'].split('-'))
    res = float(options['forecast']['resolution'])
    nens = int(options['dssat']['ensemble size'])
    name = options['forecast']['name'].lower()
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
    """Driver routine for a forecast simulation."""
    forecast_options = options['forecast']
    if 'model' in forecast_options:
        if 'vic' in forecast_options['model']:
            if 'vic' in options:
                runVIC(dbname, options)
            else:
                print "ERROR! No configuration options for VIC model."
                sys.exit()
        if 'dssat' in forecast_options['model']:
            if 'dssat' in options:
                runDSSAT(dbname, options)
            else:
                print "ERROR! No configuration options for DSSAT model."
                sys.exit()
    else:
        print "ERROR! No model selected for forecast."
        sys.exit()
