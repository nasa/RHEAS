""" Class definition for the DSSAT rice interface

.. module:: dssat
   :synopsis: Definition of the DSSAT rice class

.. moduleauthor:: Kostas Andreadis <kandread@jpl.nasa.gov>

"""

from dssat import DSSAT


class Model(DSSAT):

    def _writeFileNames(fout, ens):
        """Write file name section in DSSAT control file."""
        fout.write("*MODEL INPUT FILE            B     1     1     5   999     0\r\n")
        fout.write("*FILES\r\n")
        fout.write("MODEL          RICER040\r\n")
        fout.write("FILEX          IRMZ8601.RIX\r\n")
        fout.write("FILEA          IRMZ8601.RIA\r\n")
        fout.write("FILET          IRMZ8601.RIT\r\n")
        fout.write("SPECIES        RICER040.SPE\r\n")
        fout.write("ECOTYPE        RICER040.ECO\r\n")
        fout.write("CULTIVAR       RICER040.CUL\r\n")
        fout.write("PESTS          RICER040.PST\r\n")
        fout.write("SOILS          SOIL.SOL\r\n")
        fout.write("WEATHER        WEATH{0:03d}.WTH\r\n".format(ens+1))
        fout.write("OUTPUT         OVERVIEW\r\n")

    def _writeSimulationControl(self, fout, startdate):
        """Write simulation control section in DSSAT control file."""
        fout.write("*SIMULATION CONTROL\r\n")
        fout.write("                   1     1     S {0}  2150 IRRI MUNOZ JAN 86 UREASE  RICER\r\n".format(startdate.strftime("%Y%j")))
        fout.write("                   Y     Y     N     N     N     N     N     N\r\n")
        fout.write("                   M     M     E     R     S     C     R     1     G\r\n")
        fout.write("                   R     R     R     R     M\r\n")
        fout.write("                   N     Y     Y     1     Y     N     Y     Y     N     N     Y     N     N\r\n")

    def _writeAutomaticMgmt(self, fout):
        """Write automatic management section in DSSAT control file."""
        fout.write("!AUTOMATIC MANAGEM\r\n")
        fout.write("               1986029 1986043   40.  100.   30.   40.   10.\r\n")
        fout.write("                 30.   50.  100. IB001 IB001  10.0 1.000\r\n")
        fout.write("                 30.   50.   25. IB001 IB001\r\n")
        fout.write("                100.     1   20.\r\n")
        fout.write("                     0 1986036  100.    0.\r\n")

    def _writeExpDetails(self, fout):
        """Write experiment details section in DSSAT control file."""
        pass

    def _writeTreatments(self, fout):
        """Write treatments section in DSSAT control file."""
        pass

    def _writeCultivars(self, fout):
        """Write cultivars section in DSSAT control file."""
        fout.write("*CULTIVARS\r\n")
        fout.write("   990002 MEDIUM SEASON\r\n")

    def _writeFields(self, fout, lat, lon):
        """Write fields section in DSSAT control file."""
        pass

    def _writeInitialConditions(self, fout, startdate, dz, smi):
        """Write initial condition section in DSSAT control file."""
        pass

    def _writePlanting(self, fout, pdt):
        """Write planting details section in DSSAT control file."""
        pass

    def _writeIrrigation(self, fout, irrigation):
        """Write irrigation details section in DSSAT control file."""
        pass

    def _writeFertilizer(self, fout, fertilizers):
        """Write fertilizer section in DSSAT control file."""
        pass

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
        pass

    def _writeCultivar(self, fout, cultivar):
        """Write cultivar information in DSSAT control file."""
        fout.write("*CULTIVAR\r\n")
        fout.write(cultivar)

    def writeControlFile(self, modelpath, vsm, depths, startdate, gid, lat, lon, planting, fertilizers, irrigation):
        """Writes DSSAT control file for specific pixel."""
        pass
