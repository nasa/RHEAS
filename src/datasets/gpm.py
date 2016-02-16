""" RHEAS module for retrieving the GPM daily precipitation data product (IMERG).

.. module:: prism
   :synopsis: Retrieve GPM precipitation data

.. moduleauthor:: Kostas Andreadis <kandread@jpl.nasa.gov>

"""


from ftplib import FTP
from datetime import datetime, timedelta
import tempfile
import subprocess
import datasets
import dbio


table = "precip.gpm"


def dates(dbname):
    dts = datasets.dates(dbname, table)
    return dts


def download(dbname, dts, bbox):
    """Downloads the PRISM data products for a set of
    dates *dt* and imports them into the PostGIS database *dbname*."""
    url = "jsimpson.pps.eosdis.nasa.gov"
    ftp = FTP(url)
    # FIXME: Change to RHEAS-specific password
    ftp.login('kandread@jpl.nasa.gov', 'kandread@jpl.nasa.gov')
    ftp.cwd("data/imerg/gis")
    outpath = tempfile.mkdtemp()
    ts = list(set([(t.year, t.month) for t in [dts[0] + timedelta(dti) for dti in range((dts[1] - dts[0]).days + 1)]]))
    for t in ts:
        try:
            ftp.cwd("{0}/{1:02d}".format(t[0], t[1]))
            filenames = [f for f in ftp.nlst() if datetime.strptime(f.split(".")[-5].split("-")[0], "%Y%m%d") >= dts[0] and datetime.strptime(f.split(".")[-5].split("-")[0], "%Y%m%d") <= dts[1] and f.find("E.1day.tif") > 0]
            for fname in filenames:
                dt = datetime.strptime(fname.split(".")[-5].split("-")[0], "%Y%m%d")
                with open("{0}/{1}".format(outpath, fname), 'wb') as f:
                    ftp.retrbinary("RETR {0}".format(fname), f.write)
                with open("{0}/{1}".format(outpath, fname.replace(".tif", ".tfw")), 'wb') as f:
                    ftp.retrbinary("RETR {0}".format(fname.replace(".tif", ".tfw")), f.write)
                subprocess.call(["gdalwarp", "-t_srs", "epsg:4326", "{0}/{1}".format(outpath, fname), "{0}/prec.tif".format(outpath)])
                if bbox is not None:
                    subprocess.call(["gdal_translate", "-a_srs", "epsg:4326", "-projwin", "{0}".format(bbox[0]), "{0}".format(bbox[3]), "{0}".format(bbox[2]), "{0}".format(bbox[1]), "{0}/prec.tif".format(outpath), "{0}/prec1.tif".format(outpath)])
                else:
                    subprocess.call(["gdal_translate", "-a_srs", "epsg:4326", "{0}/prec.tif".format(outpath), "{0}/prec1.tif".format(outpath)])
                cmd = " ".join(["gdal_calc.py", "-A", "{0}/prec1.tif".format(outpath), "--outfile={0}/prec2.tif".format(outpath), "--calc=\"0.1*A\""])
                subprocess.call(cmd, shell=True)
                dbio.ingest(dbname, "{0}/prec2.tif".format(outpath), dt, table, False)
            ftp.cwd("../..")
        except:
            print("GPM data not available for {0}/{1}. Skipping download!".format(t[0], t[1]))
