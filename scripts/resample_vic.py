# Resample VIC soil, elevation and vegetation information to target spatial resolution
# from initial 0.25 degree resolution available in RHEAS

# Author: Kostas Andreadis

from scipy.interpolate import griddata
from collections import OrderedDict
from osgeo import gdal
import numpy as np


def read_soilfile(minlat, maxlat, minlon, maxlon, soilfile):
    """Read VIC soil file and extract bounding box."""
    res = 0.25
    lines = []
    with open(soilfile) as fin:
        for line in fin:
            toks = line.split()
            lat = float(toks[2])
            lon = float(toks[3])
            if lat >= minlat-res/2 and lat <= maxlat+res/2 and lon >= minlon-res/2 and lon <= maxlon+res/2:
                lines.append(line)
    lats = np.array([float(line.split()[2]) for line in lines])
    lons = np.array([float(line.split()[3]) for line in lines])
    return lines, lats, lons


def grid_data(data1, lats, lons):
    """Grid 1-D data."""
    x, y = np.meshgrid(np.sort(np.unique(lons)), np.sort(np.unique(lats))[-1::-1])
    res = x[0, 1] - x[0, 0]
    data2 = np.zeros(x.shape)
    data2[:] = np.nan
    for c in range(len(data1)):
        j = int((lons[c] - x[0, 0]) / res)
        i = int((y[0, 0] - lats[c]) / res)
        data2[i, j] = data1[c]
    return data2


def create_mask(lats, lons, nres):
    """Create mask at new spatial resolution."""
    x, y = np.meshgrid(np.sort(np.unique(lons)), np.sort(np.unique(lats))[-1::-1])
    res = x[0, 1] - x[0, 0]
    xn = np.arange(x[0, 0] - res/2 + nres/2, x[0, -1] + res/2, nres)
    yn = np.arange(y[0, 0] + res/2 - nres/2, y[-1, 0] - res/2, -nres)
    mask = np.zeros((len(yn), len(xn)), dtype='int')
    for c in range(len(lats)):
        i1 = int((yn[0] - lats[c] - res/2) / nres)
        i2 = int((yn[0] - lats[c] + res/2) / nres)
        j1 = int((lons[c] - res/2 - xn[0]) / nres) + 1
        j2 = int((lons[c] + res/2 - xn[0]) / nres) + 1
        mask[i1:i2, j1:j2] = 1
    return mask, yn, xn


def interp(data, lons, lats, nres):
    """Interpolate grid to new coordinates."""
    x, y = np.meshgrid(np.sort(np.unique(lons)), np.sort(np.unique(lats))[-1::-1])
    res = x[0, 1] - x[0, 0]
    xn = np.arange(x[0, 0] - res/2 + nres/2, x[0, -1] + res/2, nres)
    yn = np.arange(y[0, 0] + res/2 - nres/2, y[-1, 0] - res/2, -nres)
    z1 = griddata((lons, lats), data, (xn[None, :], yn[:, None]), method='linear')
    z2 = griddata((lons, lats), data, (xn[None, :], yn[:, None]), method='nearest')
    z1[np.isnan(z1)] = z2[np.isnan(z1)]
    return z1


def resample_soil(minlat, maxlat, minlon, maxlon, nres, soilfile="../data/vic/global_soil_0.25deg.txt", nlayer=3):
    """Resample VIC soil file."""
    lines, lats, lons = read_soilfile(minlat, maxlat, minlon, maxlon, soilfile)
    mask, nlat, nlon = create_mask(lats, lons, nres)
    varcols = [('infilt', [4]), ('Ds', [5]), ('Dsmax', [6]), ('Ws', [7]), ('c', [8]), ('expt', range(9, nlayer+9)), ('Ksat', range(9+nlayer, 2*nlayer+9)), ('phi_s', range(2*nlayer+9, 3*nlayer+9)), ('init_moist', range(3*nlayer+9, 4*nlayer+9)), ('elev', [4*nlayer+9]), ('depth', range(4*nlayer+10, 5*nlayer+10)), ('avg_T', [5*nlayer+10]), ('dp', [5*nlayer+11]), ('bubble', range(5*nlayer+12, 6*nlayer+12)), ('quartz', range(6*nlayer+12, 7*nlayer+12)), ('bulk_density', range(7*nlayer+12, 8*nlayer+12)), ('soil_density', range(8*nlayer+12, 9*nlayer+12)), ('off_gmt', [9*nlayer+12]), ('Wcr_fract', range(9*nlayer+13, 10*nlayer+13)), ('Wpwp_fract', range(10*nlayer+13, 11*nlayer+13)), ('rough', [11*nlayer+13]), ('snow_rough', [11*nlayer+14]), ('annual_prec', [11*nlayer+15]), ('resid_moist', range(11*nlayer+16, 12*nlayer+16)), ('fs_active', [12*nlayer+16])]
    varcols = OrderedDict(varcols)
    rdata = {}
    for k in varcols:
        rdata[k] = []
        for l in varcols[k]:
            data = np.array([float(line.split()[l]) for line in lines])
            z = interp(data, lons, lats, nres)
            rdata[k].append(z)
    rdata['fs_active'] = [rdata['fs_active'][0].astype('int')]
    rsoilfile = soilfile.replace("0.25", str(nres))
    cellnum = 1
    with open(rsoilfile, 'w') as fout:
        for i in range(mask.shape[0]):
            for j in range(mask.shape[1]):
                if mask[i, j] == 1:
                    line = "1 {0} {1:.5f} {2:.5f}".format(cellnum, nlat[i], nlon[j])
                    for k in varcols:
                        for l in range(len(rdata[k])):
                            line += " {0}".format(rdata[k][l][i, j])
                    fout.write(line+"\n")
                    cellnum += 1


def resample_snowbands(elevfile, soilfile, snowbandfile, nres, nbands=1):
    """Resample elevation raster and generate VIC snowbands file."""
    f = gdal.Open(elevfile)
    xul, xres, _, yul, _, yres = f.GetGeoTransform()
    dem = f.ReadAsArray()
    f = None
    with open(soilfile) as fin, open(snowbandfile, 'w') as fout:
        for line in fin:
            toks = line.split()
            lat = float(toks[2])
            lon = float(toks[3])
            i1 = int((lat+nres/2 - yul) / yres)
            i2 = int((lat-nres/2 - yul) / yres)
            j1 = int((lon-nres/2 - xul) / xres)
            j2 = int((lon+nres/2 - xul) / xres)
            area = np.zeros(nbands)
            elev = np.zeros(nbands)
            dp = 100.0 / nbands
            z = dem[i1:i2+1, j1:j2+1].ravel()
            z = z[~np.isnan(z)]
            for pi, p in enumerate(np.arange(0, 100, dp)):
                k = np.where(np.logical_and(z >= np.percentile(z, p), z < np.percentile(z, p+dp)))[0]
                if len(k) == 0:
                    k = np.where(z < np.percentile(z, p+dp) + 0.1)  # add elevation tolerance to catch cases of entirely flat terrain
                area[pi] = len(k) / float(len(z))
                elev[pi] = np.mean(z[k])
            area /= np.sum(area)
            fout.write("{0}".format(int(toks[1])))
            for b in range(nbands):
                fout.write(" {0:f}".format(area[b]))
            for b in range(nbands):
                fout.write(" {0:f}".format(elev[b]))
            for b in range(nbands):
                fout.write(" {0:f}".format(area[b]))
            fout.write("\n")
