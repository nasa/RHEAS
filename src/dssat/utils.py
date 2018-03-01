""" Module for helper function of the DSSAT model

.. module:: utils
   :synopsis: Definition of the DSSAT model utility functions

.. moduleauthor:: Kostas Andreadis <kandread@jpl.nasa.gov>

"""

import string
import subprocess
import rpath
import dbio
import random
import os


def addCultivar(dbname, shapefile, params, nens=40, crop="maize"):
    """Add cultivar parameters to the database *dbname* corresponding
    to the area defined in the *shapefile*. The *params* is a list of dictionaries,
    where the keys of each dictionary correspond to parameters, and each object in
    the list corresponds to a cultivar variant. The *nens* parameters is the size
    of the ensemble to be created."""
    temptable = ''.join(random.SystemRandom().choice(
        string.ascii_letters) for _ in range(8))
    if os.path.exists(shapefile):
        subprocess.call("{0}/shp2pgsql -d -s 4326 -g geom {1} {2} | {0}/psql -d {3}".format(
            rpath.bins, shapefile, temptable, dbname), shell=True)
        db = dbio.connect(dbname)
        cur = db.cursor()
        e = 0
        while e < nens:
            for c in range(len(params)):
                if crop == "maize" and all(p in params[c] for p in ['p1', 'p2', 'p5', 'g2', 'g3', 'phint']):
                    if e < nens:
                        sql = "insert into dssat.cultivars (geom) (select geom from {0})".format(
                            temptable)
                        cur.execute(sql)
                        sql = "update dssat.cultivars set crop='maize',ensemble={0},{1} where ensemble is null".format(
                            e + 1, ",".join(["{0}={1}".format(k, params[c][k]) for k in params[c]]))
                        cur.execute(sql)
                        e += 1
                elif crop == "rice" and all(p in params[c] for p in ['p1', 'p2r', 'p5', 'p2o', 'g1', 'g2', 'g3', 'g4']):
                    if e < nens:
                        sql = "insert into dssat.cultivars (geom) (select geom from {0})".format(
                            temptable)
                        cur.execute(sql)
                        sql = "update dssat.cultivars set crop='rice',ensemble={0},{1} where ensemble is null".format(
                            e + 1, ",".join(["{0}={1}".format(k, params[c][k]) for k in params[c]]))
                        cur.execute(sql)
                        e += 1
                else:
                    print("Missing parameters for {0} crop".format(crop))
                    params.pop(c)  # remove element with missing parameters
                    break
        cur.execute("drop table {0}".format(temptable))
        db.commit()
        cur.close()
        db.close()
    else:
        print(
            "Shapefile {0} cannot be found. Not adding cultivars!".format(shapefile))
