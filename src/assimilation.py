""" RHEAS module for assimilation

.. module:: assimilation
   :synopsis: Definition of the assimilation module

.. moduleauthor:: Kostas Andreadis <kandread@jpl.nasa.gov>

"""

import kalman
from datetime import date
import numpy as np
from collections import OrderedDict
from scipy.spatial.distance import cdist
from functools import partial
import re
import dbio
import logging


def observationDates(obsnames, dbname, startyear, startmonth, startday, endyear, endmonth, endday, update):
    """Return dates when observation *obsname* is available during the
    simulation period."""
    if update is not None and isinstance(update, str):
        if update.find("week") >= 0:
            update = 7
        elif update.find("month") >= 0:
            update = 30
        else:
            update = -1
    else:
        update = 1
    dates = []
    db = dbio.connect(dbname)
    cur = db.cursor()
    for name in obsnames:
        name = name.lower().strip()
        obsmod = __import__("datasets." + name, fromlist=[name])
        obsobj = getattr(obsmod, name.capitalize())
        obs = obsobj()
        sql = "select distinct(fdate) from {0} where fdate>=date '{1}-{2}-{3}' and fdate<=date '{4}-{5}-{6}'".format(
            obs.tablename, startyear, startmonth, startday, endyear, endmonth, endday)
        cur.execute(sql)
        results = cur.fetchall()
        for ri, r in enumerate(results):
            if not r[0] in dates:
                if isinstance(update, date) and r[0] is update:
                    dates.append(r[0])
                elif isinstance(update, int):
                    if (ri > 0 and (r[0] - dates[-1]).days >= update) or ri < 1:
                        dates.append(r[0])
                else:
                    dates.append(r[0])
    dates.sort()
    for dt in [date(startyear, startmonth, startday), date(endyear, endmonth, endday)]:
        if dt in dates:
            # remove first and last day of simulation since it will not impact
            # results saved
            dates.remove(dt)
    cur.close()
    db.close()
    return dates


def assimilate(options, dt, models, method="letkf"):
    """Assimilate multiple observations into the VIC model."""
    log = logging.getLogger(__name__)
    obsnames = options['vic']['observations'].split(",")
    X = OrderedDict()
    Xlat = OrderedDict()
    Xlon = OrderedDict()
    Xgid = OrderedDict()
    HX = OrderedDict()
    Y = OrderedDict()
    Ylat = OrderedDict()
    Ylon = OrderedDict()
    for name in obsnames:
        name = name.lower().strip()
        # dynamically load observation module and get data
        obsmod = __import__("datasets." + name, fromlist=[name])
        obsobj = getattr(obsmod, name.capitalize())
        # check whether user has set uncertainty parameters for observation
        if 'observations' in options and name in options['observations']:
            try:
                sname = re.split(" |,", options['observations']['name]'])[0].lower()
                params = map(float, re.split(" |,", options['observations']['name]'])[1:])
                smod = __import__("scipy.stats", fromlist=[sname])
                sdist = getattr(smod, sname)
            except:
                log.warning("No distribution {0} available for dataset {1}, falling back to default.".format(sname, name))
            else:
                rvs = partial(sdist.rvs, *params)
                obs = obsobj(rvs)
        else:
            obs = obsobj()
        data, lat, lon = obs.get(dt, models)
        if data is not None:
            if obs.obsvar not in Y:
                Y[obs.obsvar] = data
                Ylat[obs.obsvar] = lat[:, 0]
                Ylon[obs.obsvar] = lon[:, 0]
            data, lat, lon, gid = obs.x(dt, models)
            for s in obs.statevar:
                if s not in X:
                    X[s] = data[s]
                    Xlat[s] = lat[:, 0]
                    Xlon[s] = lon[:, 0]
                    Xgid[s] = gid[:, 0]
            data, _, _ = obs.hx(models, dt)
            if obs.obsvar not in HX:
                HX[obs.obsvar] = data
    if bool(X):
        x = np.vstack((X[k] for k in X))
        hx = np.vstack((HX[k] for k in HX))
        y = np.vstack((Y[k] for k in Y))
        xlat = np.vstack((Xlat[k] for k in Xlat))
        xlon = np.vstack((Xlon[k] for k in Xlon))
        ylat = np.vstack((Ylat[k] for k in Ylat))
        ylon = np.vstack((Ylon[k] for k in Ylon))
        dists = cdist(np.vstack((xlat, xlon)).T, np.vstack((ylat, ylon)).T)
        kfobj = getattr(kalman, method.upper())
        E = obs.E(models.nens)
        kf = kfobj(x, hx, y, E)
        kf.analysis(dists)
        i = 0
        for k in X:
            for j in range(i, X[k].shape[0] + i):
                X[k][j - i, :] = kf.Aa[j, :]
            i += X[k].shape[0]
    return X, Xlat, Xlon, Xgid
