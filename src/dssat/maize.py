""" Class definition for the DSSAT maize interface

.. module:: dssat
   :synopsis: Definition of the DSSAT maize class

.. moduleauthor:: Kostas Andreadis <kandread@jpl.nasa.gov>

"""

from dssat import DSSAT
from datetime import timedelta
import dbio


class Model(DSSAT):

    def __init__(self, *args, **kwargs):
        super(Model, self).__init__(*args, **kwargs)
        self.crop = "maize"

    def _writeFileNames(self, fout, ens):
        """Write file name section in DSSAT control file."""
        fout.write("*MODEL INPUT FILE            A     1     1     1     6     0\r\n")
        fout.write("*FILES\r\n")
        fout.write("MODEL          MZCER_Ex\r\n")
        fout.write("FILEX          NOZI0901.MZX\r\n")
        fout.write("FILEA          NOZI0901.MZA\r\n")
        fout.write("FILET          NOZI0901.MZT\r\n")
        fout.write("SPECIES        MZCER_Ex.SPE\r\n")
        fout.write("ECOTYPE        MZCER_Ex.ECO\r\n")
        fout.write("CULTIVAR       MZCER_Ex.CUL\r\n")
        fout.write("PESTS          MZCER_Ex.PST\r\n")
        fout.write("SOILS          SOIL.SOL\r\n")
        fout.write("WEATHER        WEATH{0:03d}.WTH\r\n".format(ens+1))
        fout.write("OUTPUT         NOZI0901\r\n")

    def _writeSimulationControl(self, fout, startdate):
        """Write simulation control section in DSSAT control file."""
        fout.write("*SIMULATION CONTROL\r\n")
        fout.write("                   1     1     S {0}  2150 N X IRRIGATION, AF_LOC MZCER\r\n".format(startdate.strftime("%Y%j")))
        fout.write("                   Y     Y     N     N     N     N     N     N\r\n")
        fout.write("                   M     M     E     R     S     C     R     1     G\r\n")
        fout.write("                   R     R     R     N     M\r\n")
        fout.write("                   Y     Y     Y     1     Y     N     Y     Y     N     N     D     N     N\r\n")

    def _writeAutomaticMgmt(self, fout, startdate):
        """Write automatic management section in DSSAT control file."""
        t0 = startdate - timedelta(3)
        t1 = t0 + timedelta(14)
        fout.write("!AUTOMATIC MANAGEM\r\n")
        fout.write("               {0} {1}   40.  100.   30.   40.   10\r\n".format(t0.strftime("%Y%j"), t1.strftime("%Y%j")))
        fout.write("                 30.   50.  100. GS000 IR001  10.0 1.000\r\n")
        fout.write("                 30.   50.   25. FE001 GS000\r\n")
        fout.write("                100.     1   20.\r\n")
        fout.write("                     0 2009130  100.    0.\r\n")

    def _writeExpDetails(self, fout):
        """Write experiment details section in DSSAT control file."""
        fout.write("*EXP.DETAILS\r\n")
        fout.write("  1NOZI0901 MZ NIT X IRR, AF_LOC 2N*3I\r\n")

    def _writeTreatments(self, fout):
        """Write treatments section in DSSAT control file."""
        fout.write("*TREATMENTS\r\n")
        fout.write("  1 1 0 0 RAINFED LOW NITROGEN\r\n")

    def _writeCultivars(self, fout):
        """Write cultivars section in DSSAT control file."""
        fout.write("*CULTIVARS\r\n")
        fout.write("   990002 MEDIUM SEASON\r\n")

    def _writeFields(self, fout, lat, lon):
        """Write fields section in DSSAT control file."""
        fout.write("*FIELDS\r\n")
        fout.write("   AF0000 NOZI0901   0.0    0. DR000    0.  100. 00000        180. AFPN930001\r\n")
        fout.write("          {0:8.5f}       {1:10.5f}     40.               1.0  100.   1.0   1.0\r\n".format(lat, lon))

    def _writeInitialConditions(self, fout, startdate, dz, smi):
        """Write initial condition section in DSSAT control file."""
        fout.write("*INITIAL CONDITIONS\r\n")
        fout.write("   MZ    {0}  100.    0.  1.00  1.00   0.0  1000  0.80  0.00  100.   15.\r\n".format(startdate.strftime("%Y%j")))
        for lyr in range(len(dz)):
            fout.write("{0:8.0f}{1:8.3f}{2:8.1f}{3:8.1f}\r\n".format(dz[lyr], smi[0, lyr], 0.5, 0.1))

    def _writePlanting(self, fout, pdt):
        """Write planting details section in DSSAT control file."""
        fout.write("*PLANTING DETAILS\r\n")
        fout.write("   {0}     -99   4.4   4.4     S     R   61.    0.   7.0  -99.  -99. -99.0 -99.0   0.0\r\n".format(pdt.strftime("%Y%j")))

    def _writeIrrigation(self, fout, irrigation):
        """Write irrigation details section in DSSAT control file."""
        fout.write("*IRRIGATION\r\n")
        fout.write("   1.000   30.   75.  -99. GS000 IR001   0.0\r\n")
        for i, irrig in enumerate(irrigation):
            fout.write("   {0} IR{1:03d} {2:4.1f}\r\n".format(irrig[0].strftime("%Y%j"), i+1, irrig[1]))

    def _writeFertilizer(self, fout, fertilizers):
        """Write fertilizer section in DSSAT control file."""
        fout.write("*FERTILIZERS\r\n")
        for f, fert in enumerate(fertilizers):
            dt, fe, ap, depth, amount = fert
            fout.write("   {0} FE{1} AP{2}   {3:.0f}.   {4:.0f}.    0.    0.    0.    0.   -99\r\n".format(dt.strftime("%Y%j"), fe, ap, depth, amount))

    def _writeResidues(self, fout):
        """Write residues section in DSSAT control file."""
        fout.write("*RESIDUES\r\n")

    def _writeChemicals(self, fout):
        """Write chemicals section in DSSAT control file."""
        fout.write("*CHEMICALS\r\n")

    def _writeTillage(self, fout):
        """Write tillage section in DSSAT control file."""
        fout.write("*TILLAGE\r\n")

    def _writeEnvironment(self, fout):
        """Write environment section in DSSAT control file."""
        fout.write("*ENVIRONMENT\r\n")

    def _writeHarvest(self, fout):
        """Write chemicals section in DSSAT control file."""
        fout.write("*HARVEST\r\n")

    def _writeSoil(self, fout, prof, dz):
        """Write soil section in DSSAT control file."""
        fout.write("*SOIL\r\n")
        for ln in prof[:-1]:
            fout.write(ln+"\r\n")
        fout.write("\r\n")
        for z in dz:
            fout.write("{0:6.0f}   0.0   0.0   0.0   0.0   0.0   0.0   0.0   0.0   0.0   0.0   0.0   0.0   0.0   0.0   0.0\r\n".format(z))

    def _writeCultivar(self, fout, cultivar):
        """Write cultivar information in DSSAT control file."""
        fout.write("*CULTIVAR\r\n")
        fout.write(cultivar)

    def cultivar(self, ens, gid):
        """Retrieve Cultivar parameters for pixel and ensemble member."""
        db = dbio.connect(self.dbname)
        cur = db.cursor()
        if dbio.columnExists(self.dbname, "dssat", "cultivars", "name"):
            name_query = ",c.name"
        else:
            name_query = ""
        sql = "select p1,p2,p5,g2,g3,phint{3} from dssat.cultivars as c,{0}.agareas as a where crop='maize' and ensemble={1} and st_intersects(c.geom,a.geom) and a.gid={2}".format(self.name, ens + 1, gid, name_query)
        cur.execute(sql)
        if not bool(cur.rowcount):
            sql = "select p1,p2,p5,g2,g3,phint{3} from dssat.cultivars as c,{0}.agareas as a where crop='maize' and ensemble={1} and a.gid={2} order by st_centroid(c.geom) <-> st_centroid(a.geom)".format(self.name, ens + 1, gid, name_query)
            cur.execute(sql)
        if name_query:
            p1, p2, p5, g2, g3, phint, cname = cur.fetchone()
        else:
            p1, p2, p5, g2, g3, phint = cur.fetchone()
            cname = ""
        # FIXME: Should the name of the cultivar be reflected in the line below?
        cultivar = "990002 MEDIUM SEASON    IB0001  {0:.1f} {1:.3f} {2:.1f} {3:.1f}  {4:.2f} {5:.2f}".format(p1, p2, p5, g2, g3, phint)
        cur.close()
        db.close()
        self.cultivars[gid].append(cname)
        return cultivar

    def writeControlFile(self, modelpath, vsm, depths, startdate, gid, lat, lon, planting, fertilizers, irrigation):
        """Writes DSSAT control file for specific pixel."""
        if isinstance(vsm, list):
            vsm = (vsm * (int(self.nens / len(vsm)) + 1))[:self.nens]
        else:
            vsm = [vsm] * self.nens
        profiles = self.sampleSoilProfiles(gid)
        profiles = [p[0] for p in profiles]
        self.cultivars[gid] = []
        for ens in range(self.nens):
            sm = vsm[ens]
            fertilizers = [(planting, "005", "001", 1.0, 60.0), (planting+timedelta(30), "005", "001", 1.0, 70.0), (planting+timedelta(45), "005", "001", 1.0, 80.0)] if fertilizers is None else fertilizers
            irrigation = [(startdate, 0.0)] if irrigation is None else irrigation
            prof = profiles[ens].split("\r\n")
            dz = map(lambda ln: float(ln.split()[0]), profiles[ens].split("\n")[3:-1])
            smi = self.interpolateSoilMoist(sm, depths, dz)
            cultivar = self.cultivar(ens, gid)
            filename = "{0}/DSSAT{1}_{2:03d}.INP" .format(modelpath, self.nens, ens + 1)
            with open(filename, 'w') as fout:
                self._writeFileNames(fout, ens)
                self._writeSimulationControl(fout, startdate)
                self._writeAutomaticMgmt(fout, startdate)
                self._writeExpDetails(fout)
                self._writeTreatments(fout)
                self._writeCultivars(fout)
                self._writeFields(fout, lat, lon)
                self._writeInitialConditions(fout, startdate, dz, smi)
                self._writePlanting(fout, planting)
                self._writeIrrigation(fout, irrigation)
                self._writeFertilizer(fout, fertilizers)
                self._writeResidues(fout)
                self._writeChemicals(fout)
                self._writeTillage(fout)
                self._writeEnvironment(fout)
                self._writeHarvest(fout)
                self._writeSoil(fout, prof, dz)
                self._writeCultivar(fout, cultivar)
        return dz, smi

    def yieldTable(self):
        """Create table for crop yield statistics and crop type."""
        super(Model, self).yieldTable()
        db = dbio.connect(self.dbname)
        cur = db.cursor()
        sql = "update {0}.yield set crop='maize' where crop is null".format(self.name)
        cur.execute(sql)
        db.commit()
        cur.close()
        db.close()
