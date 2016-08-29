#!/data/rheas/RHEAS/bin/rheaspy
# CHANGE THE INTERPRETER TO LOCAL INSTALLATION


import config
import nowcast
import logging
import sys
from datetime import datetime
from dateutil.relativedelta import relativedelta


def vic(data_stream="stable"):
    dbname = "rheas"
    options = config.loadFromFile("/data/rheas/RHEAS/nowcast.conf")
    today = datetime.today()
    if data_stream == "nrt":  # runs every Wednesday
        logging.basicConfig(filename="/data/rheas/RHEAS/log/nowcast_nrt.{0}".format(today.strftime("%Y%m%d")), level=logging.INFO, format='%(levelname)s: %(messa\
ge)s')
        startdate = today - relativedelta(days=10)
        enddate = today - relativedelta(days=3)
        options['vic']['precipitation'] = "gpm"
    else:
        # runs on first of every month
        logging.basicConfig(filename="/data/rheas/RHEAS/log/nowcast_stable.{0}".format(today.strftime("%Y%m%d")), level=logging.INFO, format='%(levelname)s: %(me\
ssage)s')
        startdate = datetime(today.year, today.month, 1) - relativedelta(months=2)
        enddate = datetime(today.year, today.month, 1) - relativedelta(months=1) - relativedelta(days=1)
        options['vic']['precipitation'] = "chirps"
    options['nowcast']['startdate'] = startdate.strftime("%Y-%m-%d")
    options['nowcast']['enddate'] = enddate.strftime("%Y-%m-%d")
    options['nowcast']['name'] = "eafrica_{0}".format(data_stream)
    nowcast.execute(dbname, options)


def dssat(data_stream="stable"):
    dbname = "rheas"
    options = config.loadFromFile("/data/rheas/RHEAS/nowcast.conf")
    options['nowcast']['model'] = "vic, dssat"
    today = datetime.today()
    if data_stream == "nrt":  # runs every Wednesday
        logging.basicConfig(filename="/data/rheas/RHEAS/log/nowcast_nrt.{0}".format(today.strftime("%Y%m%d")), level=logging.INFO, format='%(levelname)s: %(messa\
ge)s')
        startdate = datetime(today.year, 1, 1)
        enddate = today - relativedelta(days=3)
    else:
        # runs on first of every month
        logging.basicConfig(filename="/data/rheas/RHEAS/log/nowcast_stable.{0}".format(today.strftime("%Y%m%d")), level=logging.INFO, format='%(levelname)s: %(me\
ssage)s')
        startdate = datetime(today.year, 1, 1)
        enddate = datetime(today.year, today.month, 1) - relativedelta(months=1) - relativedelta(days=1)
    options['nowcast']['startdate'] = startdate.strftime("%Y-%m-%d")
    options['nowcast']['enddate'] = enddate.strftime("%Y-%m-%d")
    options['nowcast']['name'] = "eafrica_{0}".format(data_stream)
    nowcast.execute(dbname, options)


if __name__ == '__main__':
    data_stream = sys.argv[1]
    vic(data_stream)
    dssat(data_stream)
