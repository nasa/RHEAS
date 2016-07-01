""" RHEAS module for retrieving meteorological forecasts/hindcasts
from the NMME model suite.

.. module:: nmme
   :synopsis: Retrieve NMME meteorological forecast data

.. moduleauthor:: Kostas Andreadis <kandread@jpl.nasa.gov>

"""

import datasets
import rpath
import dbio
import subprocess
import tempfile
import os
import shutil
import zipfile
from datetime import timedelta


def dates(dbname):
    dts = datasets.dates(dbname, "precip.nmme")
    return dts


def _writeCservConfig(bbox, startdate, enddate, varname, ens):
    """Write ClimateSERV configuration file."""
    with tempfile.NamedTemporaryFile(dir=".", delete=False) as fcfg:
        fcfg.write("[DEFAULT]\n")
        fcfg.write("APIAccessKey = 1dd4d855e8b64a35b65b4841dcdbaa8b_as\n")
        fcfg.write("DatasetType = Seasonal_Forecast\n")
        fcfg.write("OperationType = Download\n")
        fcfg.write("EarliestDate = {0}\n".format(startdate.strftime("%m/%d/%Y")))
        if (enddate - startdate).days > 180:
            enddate = startdate + timedelta(180)
            print("WARNING! NMME forecast range cannot be longer than 180 days. Resetting end date!")
        fcfg.write("LatestDate = {0}\n".format(enddate.strftime("%m/%d/%Y")))
        fcfg.write("SeasonalEnsemble = ens{0:02d}\n".format(ens))
        fcfg.write("SeasonalVariable = {0}\n".format(varname))
        coords = "[{0},{1}],[{2},{1}],[{2},{3}],[{0},{3}],[{0},{1}]".format(*bbox)
        fcfg.write("GeometryCoords = [{0}]\n".format(coords))
        fcfg.write("BaseURL = http://climateserv.nsstc.nasa.gov/chirps/scriptAccess/")
    return fcfg.name


def ingest(dbname, varname, filename, dt, ens):
    """Imports Geotif *filename* into database *dbname*."""
    schema = {'Precipitation': 'precip', 'Temperature': 'tmax'}
    db = dbio.connect(dbname)
    cur = db.cursor()
    cur.execute(
        "select * from information_schema.tables where table_schema='{0}' and table_name='nmme'".format(schema[varname]))
    if not bool(cur.rowcount):
        cur.execute("create table {0}.nmme (rid serial not null primary key, fdate date, ensemble int, rast raster)".format(
            schema[varname]))
        db.commit()
    cur.execute("select * from {0}.nmme where fdate='{1}' and ensemble = {2}".format(schema[varname], dt.strftime("%Y-%m-%d"), ens))
    if bool(cur.rowcount):
        cur.execute("delete from {0}.nmme where fdate='{1}' and ensemble = {2}".format(schema[varname], dt.strftime("%Y-%m-%d"), ens))
        db.commit()
    dbio.ingest(dbname, filename, dt, "{0}.nmme".format(schema[varname]), True, False)
    sql = "update {0}.nmme set ensemble = '{1}' where ensemble is null".format(schema[varname], ens)
    cur.execute(sql)
    db.commit()
    cur.close()
    db.close()


def download(dbname, dts, bbox=None):
    """Downloads NMME ensemble forecast data from the SERVIR ClimateSERV
    data server, and imports them into the database *dbname*. Optionally uses
    a bounding box to limit the region with [minlon, minlat, maxlon, maxlat]."""
    nens = 10
    varnames = ["Precipitation", "Temperature"]
    outpath = tempfile.mkdtemp()
    for varname in varnames:
        for e in range(nens):
            configfile = _writeCservConfig(bbox, dts[0], dts[-1], varname, e+1)
            subprocess.call(["python", "{0}/ClimateSERV_API_Access.py".format(rpath.scripts), "-config", configfile,
                             "-outfile", "{0}/{1}_{2}.zip".format(outpath, varname, e+1)])
            f = zipfile.ZipFile("{0}/{1}_{2}.zip".format(outpath, varname, e+1))
            filenames = filter(lambda s: s.endswith("tif"), f.namelist())
            f.extractall(outpath, filenames)
            for filename in filenames:
                ingest(dbname, varname, filename, dt, e+1)
            os.remove(configfile)
    shutil.rmtree(outpath)
            
