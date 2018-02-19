""" Class definition for the DSSAT maize interface

.. module:: dssat
   :synopsis: Definition of the DSSAT maize class

.. moduleauthor:: Kostas Andreadis <kandread@jpl.nasa.gov>

"""

from dssat import DSSAT
# import logging


class Model(DSSAT):

    def _writeFileNames(self, fout, ens):
        """Write file name section in DSSAT control file."""
        fout.write("*MODEL INPUT FILE            A     1     1     1     6     0\n")
        fout.write("*FILES\n")
        fout.write("MODEL          MZCER_Ex\n")
        fout.write("FILEX          NOZI0901.MZX\n")
        fout.write("FILEA          NOZI0901.MZA\n")
        fout.write("FILET          NOZI0901.MZT\n")
        fout.write("SPECIES        MZCER_Ex.SPE\n")
        fout.write("ECOTYPE        MZCER_Ex.ECO\n")
        fout.write("CULTIVAR       MZCER_Ex.CUL\n")
        fout.write("PESTS          MZCER_Ex.PST\n")
        fout.write("SOILS          SOIL.SOL\n")
        fout.write("WEATHER        WEATH{0:03d}.WTH\n".format(ens+1))
        fout.write("OUTPUT         NOZI0901\n")

    def _writeSimulationControl(self, fout, startdate):
        """Write simulation control section in DSSAT control file."""
        fout.write("*SIMULATION CONTROL\n")
        fout.write("                   1     1     S {0}  2150 N X IRRIGATION, AF_LOC MZCER\n".format(startdate.strftime("%Y%j")))
        fout.write("                   Y     Y     N     N     N     N     N     N\n")
        fout.write("                   M     M     E     R     S     C     R     1     G\n")
        fout.write("                   R     R     R     N     M\n")
        fout.write("                   Y     Y     Y     1     Y     N     Y     Y     N     N     D     N     N\n")

    def _writeAutomaticMgmt(self, fout):
        """Write automatic management section in DSSAT control file."""
        fout.write("!AUTOMATIC MANAGEM\n")
        fout.write("               2009050 2009064   40.  100.   30.   40.   10\n")
        fout.write("                 30.   50.  100. GS000 IR001  10.0 1.000\n")
        fout.write("                 30.   50.   25. FE001 GS000\n")
        fout.write("                100.     1   20.\n")
        fout.write("                     0 2009130  100.    0.\n")

    def _writeExpDetails(self, fout):
        """Write experiment details section in DSSAT control file."""
        fout.write("*EXP.DETAILS\n")
        fout.write("  1NOZI0901 MZ NIT X IRR, AF_LOC 2N*3I\n")

    def _writeTreatments(self, fout):
        """Write treatments section in DSSAT control file."""
        fout.write("*TREATMENTS\n")
        fout.write("  1 1 0 0 RAINFED LOW NITROGEN\n")

    def _writeCultivars(self, fout):
        """Write cultivars section in DSSAT control file."""
        fout.write("*CULTIVARS\n")
        fout.write("   990002 MEDIUM SEASON\n")

    def _writeFields(self, fout, lat, lon):
        """Write fields section in DSSAT control file."""
        fout.write("*FIELDS\n")
        fout.write("   AF0000 NOZI0901   0.0    0. DR000    0.  100. 00000        180. AFPN930001\n")
        fout.write("          {0:8.5f}       {1:10.5f}     40.               1.0  100.   1.0   1.0\n".format(lat, lon))

    def _writeInitialConditions(self, fout, startdate, dz, smi):
        """Write initial condition section in DSSAT control file."""
        fout.write("*INITIAL CONDITIONS\n")
        fout.write("   MZ    2009032  100.    0.  1.00  1.00   0.0  1000  0.80  0.00  100.   15.\n".format(startdate.strftime("%y%j")))
        for lyr in range(len(dz)):
            fout.write("{0:8.0f}{1:8.3f}{2:8.1f}{3:8.1f}\n".format(dz[lyr], smi[0, lyr], 0.5, 0.1))

    def _writePlanting(self, fout, pdt):
        """Write planting details section in DSSAT control file."""
        fout.write("*PLANTING DETAILS\n")
        fout.write("   {0}     -99   4.4   4.4     S     R   61.    0.   7.0  -99.  -99. -99.0 -99.0   0.0\n".format(pdt.strftime("%Y%j")))

    def _writeIrrigation(self, fout, irrigdates):
        """Write irrigation details section in DSSAT control file."""
        fout.write("*IRRIGATION\n")
        fout.write("   1.000   30.   75.  -99. GS000 IR001   0.0\n")
        for dt in irrigdates:
            fout.write("   {0} IR001  00.0\n".format(dt.strftime("%y%j")))

    def _writeFertilizer(self, fout, fertildates):
        """Write fertilizer section in DSSAT control file."""
        fout.write("*FERTILIZERS\n")
        for dt in fertildates:
            fout.write("   {0} FE001 AP001   30.   20.    0.    0.    0.    0.   -99\n".format(dt.strftime("%Y%j")))

    def _writeResidues(self, fout):
        """Write residues section in DSSAT control file."""
        fout.write("*RESIDUES\n")

    def _writeChemicals(self, fout):
        """Write chemicals section in DSSAT control file."""
        fout.write("*CHEMICALS\n")

    def _writeTillage(self, fout):
        """Write tillage section in DSSAT control file."""
        fout.write("*TILLAGE\n")

    def _writeEnvironment(self, fout):
        """Write environment section in DSSAT control file."""
        fout.write("*ENVIRONMENT\n")

    def _writeHarvest(self, fout):
        """Write chemicals section in DSSAT control file."""
        fout.write("*HARVEST\n")

    def _writeSoil(self, fout, prof, dz):
        """Write soil section in DSSAT control file."""
        fout.write("*SOIL\n")
        fout.write(" AFPN930001  SCS         CL      180 Africa\n")
        fout.write(" AF_LOC Africa          42.035 -92.630 Clayey, Clay-Loam\n")
        for ln in range(len(prof) - 1):
            fout.write(prof[ln] + "\n")
            fout.write("\n")
            for z in dz:
                fout.write("{0:6.0f}   0.0   0.0   0.0   0.0   0.0   0.0   0.0   0.0   0.0   0.0   0.0   0.0   0.0   0.0   0.0\n".format(z))

    def _writeCultivar(self, fout, cultivar):
        """Write cultivar information in DSSAT control file."""
        fout.write("*CULTIVAR\n")
        fout.write(cultivar)

    def writeControlFile(self, modelpath, vsm, depths, startdate, gid, lat, lon, planting, fertildates, irrigdates):
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
            fertilizers = fertildates
            irrigation = irrigdates
            prof = profiles[ens].split("\n")
            dz = map(lambda ln: float(ln.split()[0]), profiles[ens].split("\n")[3:-1])
            smi = self.interpolateSoilMoist(sm, depths, dz)
            cultivar = self.cultivar(ens, gid)
            filename = "{0}/DSSAT{1}_{2:03d}.INP" .format(modelpath, self.nens, ens + 1)
            with open(filename, 'w') as fout:
                self._writeFileNames(fout, ens)
                self._writeSimulationControl(fout, startdate)
                self._writeAutomaticMgmt(fout)
                self._writeExpDetails(fout)
                self._writeTreatments(fout)
                self._writeCultivars(fout)
                self._writeFields(fout)
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
