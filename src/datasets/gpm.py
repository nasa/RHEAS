""" RHEAS module for retrieving the GPM daily precipitation data product (IMERG).

.. module:: prism
   :synopsis: Retrieve GPM precipitation data

.. moduleauthor:: Kostas Andreadis <kandread@jpl.nasa.gov>

"""


from ftplib import FTP
from datetime import timedelta
import tempfile
import subprocess
import datasets
import dbio
import re


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
    for dt in [dts[0] + timedelta(t) for t in range((dts[-1] - dts[0]).days+1)]:
        try:
            ftp.cwd("/data/imerg/gis/{0}/{1:02d}".format(dt.year, dt.month))
            filenames = [f for f in ftp.nlst() if re.match(r"3B.*{0}.*S000000.*1day\.tif.*".format(dt.strftime("%Y%m%d")), f) is not None]
            if len(filenames) > 0:
                fname = filenames[0]
                with open("{0}/{1}".format(outpath, fname), 'wb') as f:
                    ftp.retrbinary("RETR {0}".format(fname), f.write)
                with open("{0}/{1}".format(outpath, fname.replace("tif", "tfw")), 'wb') as f:
                    ftp.retrbinary("RETR {0}".format(fname.replace("tif", "tfw")), f.write)
                tfname = fname.replace("tif", "tfw")
                fname = datasets.uncompress(fname, outpath)
                datasets.uncompress(tfname, outpath)
                subprocess.call(["gdalwarp", "-t_srs", "epsg:4326", "{0}/{1}".format(outpath, fname), "{0}/prec.tif".format(outpath)])
                if bbox is not None:
                    subprocess.call(["gdal_translate", "-a_srs", "epsg:4326", "-projwin", "{0}".format(bbox[0]), "{0}".format(bbox[3]), "{0}".format(bbox[2]), "{0}".format(bbox[1]), "{0}/prec.tif".format(outpath), "{0}/prec1.tif".format(outpath)])
                else:
                    subprocess.call(["gdal_translate", "-a_srs", "epsg:4326", "{0}/prec.tif".format(outpath), "{0}/prec1.tif".format(outpath)])
                # multiply by 0.1 to get mm/hr and 24 to get mm/day
                cmd = " ".join(["gdal_calc.py", "-A", "{0}/prec1.tif".format(outpath), "--outfile={0}/prec2.tif".format(outpath), "--calc=\"0.1*A\""])
                subprocess.call(cmd, shell=True)
                dbio.ingest(dbname, "{0}/prec2.tif".format(outpath), dt, table, False)
        except:
            print("WARNING! No data were available to import into {0} for {1}.".format(table, dt.strftime("%Y-%m-%d")))
