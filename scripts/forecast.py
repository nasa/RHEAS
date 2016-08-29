#!/data/rheas/RHEAS/bin/rheaspy


import config
import forecast
import logging
from datetime import datetime
from dateutil.relativedelta import relativedelta


def vic():
    dbname = "rheas"
    options = config.loadFromFile("/data/rheas/RHEAS/forecast.conf")
    today = datetime.today()
    logging.basicConfig(filename="/data/rheas/RHEAS/log/forecast.{0}".format(today.strftime("%Y%m%d")), level=logging.INFO, format='%(levelname)s: %(message)s')
    startdate = datetime(today.year, today.month, 1)
    enddate = startdate + relativedelta(months=3) - relativedelta(days=1)
    options['forecast']['startdate'] = startdate.strftime("%Y-%m-%d")
    options['forecast']['enddate'] = enddate.strftime("%Y-%m-%d")
    options['forecast']['name'] = "eafrica_forecast"
    forecast.execute(dbname, options)


def dssat():
    dbname = "rheas"
    options = config.loadFromFile("/data/rheas/RHEAS/forecast.conf")
    options['forecast']['model'] = "vic, dssat"
    today = datetime.today()
    logging.basicConfig(filename="/data/rheas/RHEAS/log/forecast.{0}".format(today.strftime("%Y%m%d")), level=logging.INFO, format='%(levelname)s: %(message)s')
    startdate = datetime(today.year, 1, 1)
    enddate = startdate + relativedelta(months=3) - relativedelta(days=1)
    options['forecast']['startdate'] = startdate.strftime("%Y-%m-%d")
    options['forecast']['enddate'] = enddate.strftime("%Y-%m-%d")
    options['forecast']['name'] = "eafrica_forecast"
    forecast.execute(dbname, options)


if __name__ == '__main__':
    vic()
    dssat()
