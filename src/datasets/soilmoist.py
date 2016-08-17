"""Definition for abstract soil moisture class.

.. module:: soilmoist
   :synopsis: Definition of the Soilmoist class

.. moduleauthor:: Kostas Andreadis <kandread@jpl.nasa.gov>

"""

import numpy as np
import dbio
import logging


class Soilmoist(object):

    def __init__(self, uncert=None):
        """Initialize SMOS soil moisture object."""
        self.statevar = ["soil_moist"]
        self.obsvar = "soil_moist"
        self.uncert = uncert

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
        log = logging.getLogger(__name__)
        e = None
        if self.uncert is not None:
            try:
                e = self.uncert(size=(self.nobs, nens))
            except:
                log.warning("Error using provided parameters in observation error PDF. Reverting to default.")
        if e is None:
            e = np.random.normal(0.0, self.stddev, (self.nobs, nens))
        return e
