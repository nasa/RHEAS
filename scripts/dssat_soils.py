
# Ingest global soil information for DSSAT into RHEAS database
# Data are downloaded from https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/1PEEY0&version=2.4
# International Research Institute for Climate and Society (IRI); Michigan State University (MSU); HarvestChoice, International Food Policy Research Institute (IFPRI), 2015, "Global High-Resolution Soil Profile Database for Crop Modeling Applications", doi:10.7910/DVN/1PEEY0, Harvard Dataverse, V2

# Author: Kostas Andreadis


import subprocess
import glob
import dbio


def downloadSoilFile():
    """Download soil information file."""
    subprocess.call(["wget", "https://dataverse.harvard.edu/api/access/datafile/2717500"])
    subprocess.call(["unzip", "2717500"])


def parseSolFile(filename):
    """Parses SOL file and extract soil profiles."""
    data = {}
    profile = None
    lat = None
    lon = None
    with open(filename) as fin:
        for line in fin:
            if line.startswith("*"):
                if profile is not None:
                    data[(lat, lon)] = "{0}\r\n".format(profile)
                profile = line[1:].strip()
            elif not line.startswith("@") and len(line.strip()) > 0:
                toks = line.split()
                if len(toks) == 5:
                    lat = float(toks[2])
                    lon = float(toks[3])
                    profile += "\r\n{0}".format(line.strip())
                else:
                    try:
                        float(toks[0])
                        line = line.replace(toks[1], "".join([" "]*len(toks[1])))
                        profile += "\r\n{0}".format(line.rstrip())
                    except:
                        profile += "\r\n {0}".format(line.rstrip())
    return data


def ingestSoils(dbname="rheas"):
    """Ingest soil information from downloaded files."""
    filenames = glob.glob("SoilGrids-for-DSSAT-10km v1.0 (by country)/*.SOL")
    db = dbio.connect(dbname)
    cur = db.cursor()
    if dbio.tableExists(dbname, "dssat", "soils"):
        print("Overwriting existing DSSAT soils table in database!")
        cur.execute("drop table dssat.soils")
        db.commit()
    cur.execute("create table dssat.soils (rid serial primary key, geom geometry(Point, 4326), props text)")
    db.commit()
    for filename in filenames:
        try:
            profiles = parseSolFile(filename)
            for latlon in profiles:
                lat, lon = latlon
                sql = "insert into dssat.soils (geom, props) values (st_geomfromtext('POINT({0} {1})', 4326), '{2}')".format(lon, lat, profiles[latlon])
                cur.execute(sql)
        except:
            print("Cannot process file {0}".format(filename))
    db.commit()
    cur.close()
    db.close()


if __name__ == '__main__':
    downloadSoilFile()
    ingestSoils()
