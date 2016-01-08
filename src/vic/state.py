""" Module for the VIC model state functions

.. module:: state
   :synopsis: Definition of the VIC state module

.. moduleauthor:: Kostas Andreadis <kandread@jpl.nasa.gov>

"""


from collections import OrderedDict
import numpy as np


def readStateFile(filename):
    """Reads VIC initial state file."""
    state = OrderedDict()
    with open(filename) as fin:
        dateline = fin.readline()
        nlayer, nnodes = map(int, fin.readline().split())
        lines = fin.readlines()
        c = 0
        while c < len(lines):
            cell = lines[c].split()
            cellid, nveg, nbands = map(int, cell[:3])
            # state[cellid] = lines[c:c+(nveg+1)*nbands+nveg+2]
            # c = c + (nveg + 1)*nbands + nveg + 2
            state[cellid] = lines[c:c + (nveg + 1) * nbands + 1]
            c = c + (nveg + 1) * nbands + 1
    return state, nlayer, nnodes, dateline


def _updateSwe(x, xa, cell, nlayer, bare):
    if xa < 0.0:
        xa = 0.0
    if bare:
        k = 5 + 2 * nlayer
    else:
        k = 6 + 2 * nlayer
    data = cell.split()
    if x == 0.0:
        s = xa / 1000.0
    else:
        s = float(data[k]) * xa / x
    if xa == 0.0:
        data[k - 1] = "0.0"
        data[k - 2] = "0"
        data[k + 1] = "0.0"
        data[k + 2] = "0.0"
        data[k + 3] = "0.0"
        data[k + 4] = "0.0"
        data[k + 5] = "0.0"
        data[k + 6] = "0.0"
    if xa > 0.0 and x == 0.0:
        data[k - 1] = "1.0"
        data[k + 5] = "150.0"
        data[k + 6] = str(-2102. * 1000. * s * 273.15)
    data[k] = "{0:.6f}".format(s)
    return " ".join(data)


def _readSwe(cell, nlayer, bare):
    if bare:
        out = float(cell.split()[5 + 2 * nlayer])
    else:
        out = float(cell.split()[6 + 2 * nlayer])
    return out * 1000.0


def _updateScf(x, xa, cell, nlayer, bare):
    pass


def _readScf(cell, nlayer, bare):
    if bare:
        out = float(cell.split()[4 + 2 * nlayer])
    else:
        out = float(cell.split()[5 + 2 * nlayer])
    return out


def _readSoilMoist(cell, nlayer, bare):
    out = 0.0
    for l in range(nlayer):
        out += float(cell.split()[2 + l])
    return out


def _updateSoilMoist(x, xa, cell, nlayer, bare):
    data = cell.split()
    for l in range(nlayer):
        data[2 + l] = "{0:.6f}".format(float(data[2 + l]) * xa / x)
    return " ".join(data)


def readSnowbands(filename):
    """Read VIC elevation bands file."""
    bands = {}
    elev = {}
    with open(filename) as fin:
        for line in fin:
            data = line.split()
            nbands = (len(data) - 1) / 3
            bands[int(data[0])] = np.array(data[1:nbands + 1], 'float')
            elev[int(data[0])] = np.array(
                data[nbands + 1:2 * nbands + 1], 'float')
    return bands, elev


def readVegetation(filename):
    """Reads VIC vegetation file."""
    veg = {}
    with open(filename) as fin:
        lines = fin.readlines()
        c = 0
        while c < len(lines):
            data = lines[c].split()
            nveg = int(data[1])
            veg[int(data[0])] = np.array([float(l.split()[1])
                                          for l in lines[c + 1:c + 2 * nveg + 1:2]])
            c = c + 2 * nveg + 1
    return veg


def readVariable(model, state, alat, alon, veg, bands, nlayer, varname):
    """Reads variable from VIC initial state."""
    readfun = {'swe': _readSwe,
               'soil_moist': _readSoilMoist, 'snow_cover': _readScf}
    out = np.zeros((len(alat), 1))
    for i in range(len(alat)):
        k = model.lgid[(alat[i], alon[i])]
        nveg = len(veg[k])
        nbands = len(bands[k])
        for vi in range(nveg + 1):
            for bi in range(nbands):
                # cell = state[k][2+vi+vi*nbands+bi]
                cell = state[k][1 + vi * nbands + bi]
                if vi < nveg:
                    out[i] += veg[k][vi] * bands[k][bi] * \
                        readfun[varname](cell, nlayer, False)
                else:
                    out[i] += (1.0 - sum(veg[k])) * bands[k][bi] * \
                        readfun[varname](cell, nlayer, True)
    return out


def updateVariable(model, state, x, xa, alat, alon, agid, veg, bands, nlayer, varname):
    """Updates variable in VIC initial state."""
    updatefun = {'swe': _updateSwe,
                 'soil_moist': _updateSoilMoist, 'snow_cover': _updateScf}
    if len(x.shape) > 1:
        x = x.reshape(len(x))
    for i in range(len(alat)):
        k = model.lgid[(alat[i], alon[i])]
        nveg = len(veg[k])
        nbands = len(bands[k])
        for vi in range(nveg + 1):
            for bi in range(nbands):
                # cell = state[k][2+vi+vi*nbands+bi]
                cell = state[k][1 + vi * nbands + bi]
                if vi < nveg:
                    state[k][
                        1 + vi * nbands + bi] = updatefun[varname](x[i], xa[i], cell, nlayer, False)
                else:
                    state[k][
                        1 + vi * nbands + bi] = updatefun[varname](x[i], xa[i], cell, nlayer, True)
    return state


def writeStateFile(filename, state, header):
    """Write state file after updating variable."""
    with open(filename, 'w') as fout:
        fout.write("{0}\n".format(header))
        for k in state.keys():
            for line in state[k]:
                fout.write("{0}\n".format(line.strip()))
