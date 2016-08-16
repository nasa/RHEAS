"""Definition for abstract snow cover fraction class.

.. module:: snowcover
   :synopsis: Definition of the Snowcover class

.. moduleauthor:: Kostas Andreadis <kandread@jpl.nasa.gov>

"""

import numpy as np
import dbio
import logging


class Snowcover(object):

    def __init__(self, uncert=None):
        """Initialize MODSCAG snow cover fraction object."""
        self.statevar = ["swq"]
        self.obsvar = "snow_cover"
        self.uncert = uncert

    def x(self, dt, models):
        """Retrieve state variable from database."""
        data = {}
        db = dbio.connect(models.dbname)
        cur = db.cursor()
        for s in self.statevar:
            sql = "select ensemble,st_x(geom),st_y(geom),val from (select ensemble,(ST_PixelAsCentroids(rast)).* from {0}.{1} where fdate=date '{2}-{3}-{4}') foo group by ensemble,geom order by ensemble".format(
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
