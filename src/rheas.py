""" RHEAS module for main functionality.

.. module:: rheas
   :synopsis: Module for executing the RHEAS system

.. moduleauthor:: Kostas Andreadis <kandread@jpl.nasa.gov>

"""

import config
import nowcast
import forecast
import argparse
import dbio
import datasets
from datetime import datetime


def parseArgs():
    """Parses command line arguments and prints help messages."""
    parser = argparse.ArgumentParser(description='Runs RHEAS simulation.')
    parser.add_argument('config', help='configuration file')
    parser.add_argument('-d', metavar='DB', help='name of database to connect')
    parser.add_argument('-u', help='update database', action='store_true')
    args = parser.parse_args()
    return args.config, args.d, args.u


def update(dbname, configfile):
    """Fetch datasets and update database."""
    conf = datasets.readDatasetList(configfile)
    try:
        bbox = map(lambda s: conf.getfloat('domain', s), [
                   'minlon', 'minlat', 'maxlon', 'maxlat'])
    except:
        bbox = None
    for name in conf.sections():
        if name != 'domain':
            try:
                mod = __import__("datasets.{0}".format(name), fromlist=[name])
            except:
                mod = None
            if mod is None:
                # download generic datasets
                datasets.download(dbname, conf)
            else:
                if conf.has_option(name, 'startdate'):
                    t0 = datetime.strptime(conf.get(name, 'startdate'), "%Y-%m-%d")
                else:
                    t0 = None
                if conf.has_option(name, 'enddate'):
                    t1 = datetime.strptime(conf.get(name, 'enddate'), "%Y-%m-%d")
                else:
                    t1 = datetime.today()
                dt = mod.dates(dbname)
                if t0 is None:
                    if dt is None:
                        print("WARNING! Date information for {0} not found in the database or data.conf. Please add a startdate in the data.conf file.")
                    else:
                        dt = (dt[0], t1)
                else:
                    dt = (t0, t1)
                if dt is not None:
                    mod.download(dbname, dt, bbox)


def run():
    """Main RHEAS routine."""
    config_filename, dbname, db_update = parseArgs()
    if dbname is None:
        dbname = "rheas"
    dbio.connect(dbname)
    # check if database update is requested
    if db_update:
        print "Updating database!"
        update(dbname, config_filename)
    else:
        options = config.loadFromFile(config_filename)
        # check what simulations have been requested
        if "nowcast" in options:
            nowcast.execute(dbname, options)
        if "forecast" in options:
            forecast.execute(dbname, options)


if __name__ == '__main__':
    run()
