"""Class definition for the SMOS Soil Mositure data type.

.. module:: smos
   :synopsis: Definition of the SMOS class

.. moduleauthor:: Kostas Andreadis <kandread@jpl.nasa.gov>

"""

import dbio
import numpy as np
import os
import netCDF4 as netcdf
from datetime import datetime, timedelta
import datasets


table = "soilmoist.smos"


def dates(dbname):
    dts = datasets.dates(dbname, table)
    return dts


def download(dbname, dt, bbox=None):
    """Downloads SMOS soil mositure data for a set of dates *dt*
    and imports them into the PostGIS database *dbname*. Optionally
    uses a bounding box to limit the region with [minlon, minlat, maxlon, maxlat]."""
    res = 0.25
    url = "http://rheas:rheasjpl@cp34-bec.cmima.csic.es/thredds/dodsC/NRTSM001D025A_ALL"
    f = netcdf.Dataset(url)
    lat = f.variables['lat'][:]
    lon = f.variables['lon'][:]
    if bbox is not None:
        i = np.where(np.logical_and(lat > bbox[1], lat < bbox[3]))[0]
        j = np.where(np.logical_and(lon > bbox[0], lon < bbox[2]))[0]
        lat = lat[i]
        lon = lon[j]
    else:
        i = range(len(lat))
        j = range(len(lon))
    t0 = datetime(2010, 1, 12)  # initial date of SMOS data
    t1 = (dt[0] - t0).days
    t2 = (dt[1] - t0).days + 1
    ti = range(t1, t2)
    sm = f.variables['SM'][ti, i, j]
    # FIXME: Use spatially variable observation error
    # smv = f.variables['VARIANCE_SM'][ti, i, j]
    for tj in range(sm.shape[0]):
        filename = dbio.writeGeotif(lat, lon, res, sm[tj, :, :])
        t = t0 + timedelta(ti[tj])
        dbio.ingest(dbname, filename, t, table, False)
        print("Imported SMOS {0}".format(tj))
        os.remove(filename)


class Smos(object):

    def __init__(self):
        """Initialize SMOS soil moisture object."""
        self.statevar = ["soil_moist"]
        self.obsvar = "soil_moist"
        self.res = 0.25
        self.stddev = 0.01
        self.tablename = "soilmoist.smos"

    def x(self, dt, models):
        """Retrieve state variable from database."""
        data = {}
        db = dbio.connect(models.dbname)
        cur = db.cursor()
        for s in self.statevar:
            sql = "select ensemble,st_x(geom),st_y(geom),sum(val) from (select ensemble,layer,(ST_PixelAsCentroids(rast)).* from {0}.{1} where fdate=date '{2}-{3}-{4}') foo group by ensemble,geom order by ensemble".format(
                models.name, s, dt.year, dt.month, dt.day)
            cur.execute(sql)
            e, lon, lat, vals = zip(*cur.fetchall())
            gid = [models[0].lgid[(l[0], l[1])] for l in zip(lat, lon)]
            nens = max(e)
            data[s] = np.array(vals).reshape((len(vals) / nens, nens))
            lat = np.array(lat).reshape((len(lat) / nens, nens))
            lon = np.array(lon).reshape((len(lon) / nens, nens))
            gid = np.array(gid).reshape((len(gid) / nens, nens))
        cur.close()
        db.close()
        return data, lat, lon, gid

    def get(self, dt, models):
        """Retrieve observations from database for date *dt*."""
        db = dbio.connect(models.dbname)
        cur = db.cursor()
        sql = "select st_x(geom),st_y(geom),val from (select (st_pixelascentroids(st_clip(rast,geom))).* from {0},{1}.basin where st_intersects(rast,geom) and fdate=date '{2}-{3}-{4}') foo".format(
            self.tablename, models.name, dt.year, dt.month, dt.day)
        cur.execute(sql)
        if bool(cur.rowcount):
            lon, lat, data = zip(*cur.fetchall())
            data = np.array(data).reshape((len(data), 1))
            lat = np.array(lat).reshape((len(lat), 1))
            lon = np.array(lon).reshape((len(lon), 1))
            self.nobs = len(data)
        else:
            data = lat = lon = None
        cur.close()
        db.close()
        return data, lat, lon

    def hx(self, models, dt):
        """Retrieve observed variable from database and resample to observation resolution."""
        db = dbio.connect(models.dbname)
        cur = db.cursor()
        sql = "with f as (select st_union(st_clip(rast,geom)) as rast from {0},{1}.basin where st_intersects(rast,geom) and fdate=date '{2}-{3}-{4}') select ensemble,st_x(geom),st_y(geom),val from (select ensemble,(st_pixelascentroids(st_resample(b.rast,f.rast,'average'))).* from f,{1}.{5} as b where layer=1 and fdate=date '{2}-{3}-{4}') foo order by ensemble".format(
            self.tablename, models.name, dt.year, dt.month, dt.day, self.obsvar)
        cur.execute(sql)
        e, lon, lat, data = zip(*cur.fetchall())
        nens = max(e)
        lat = np.array(lat).reshape((len(lat) / nens, nens))
        lon = np.array(lon).reshape((len(lon) / nens, nens))
        data = np.array(data).reshape((len(data) / nens, nens))
        sql = "select depths from {0}.basin order by geom <-> st_geomfromtext('POINT(%(lon)s %(lat)s)',4326) limit 1".format(
            models.name)
        for i in range(len(data) / nens):
            for e in range(nens):
                cur.execute(sql, {'lat': lat[i, e], 'lon': lon[i, e]})
                z = cur.fetchone()[0][0]
                # convert to volumetric soil moisture
                data[i, e] /= (1000.0 * z)
        cur.close()
        db.close()
        return data, lat, lon

    def E(self, nens):
        """Generate observation error vector."""
        e = np.random.normal(0.0, self.stddev, (self.nobs, nens))
        return e
