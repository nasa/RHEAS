""" Module containing output templates for VIC.

.. module:: vic_output
   :synopsis: Definition of the VIC output module

.. moduleauthor:: Kostas Andreadis <kandread@jpl.nasa.gov>

"""

ebTemplate = """OUTFILE         eb      10
OUTVAR          OUT_NET_SHORT           %.4f    *       *
OUTVAR          OUT_NET_LONG            %.4f    *       *
OUTVAR          OUT_LATENT              %.4f    *       *
OUTVAR          OUT_SENSIBLE            %.4f    *       *
OUTVAR          OUT_GRND_FLUX           %.4f    *       *
OUTVAR          OUT_MELT_ENERGY         %.4f    *       *
OUTVAR          OUT_RFRZ_ENERGY         %.4f    *       *
OUTVAR          OUT_ADVECTION           %.4f    *       *
OUTVAR          OUT_DELTAH              %.4f    *       *
OUTVAR          OUT_DELTACC             %.4f    *       *
"""

wbTemplate = """OUTFILE         wb      8
OUTVAR          OUT_SNOWF               %.6f    *       *
OUTVAR          OUT_RAINF               %.6f    *       *
OUTVAR          OUT_EVAP                %.6f    *       *
OUTVAR          OUT_RUNOFF              %.6f    *       *
OUTVAR          OUT_BASEFLOW            %.6f    *       *
OUTVAR          OUT_SNOW_MELT           %.6f    *       *
OUTVAR          OUT_REFREEZE            %.4f    *       *
OUTVAR          OUT_PREC                %.6f    *       *
"""

surTemplate = """OUTFILE         sur     10
OUTVAR          OUT_SNOW_SURF_TEMP      %.4f    *       *
OUTVAR          OUT_VEGT                %.4f    *       *
OUTVAR          OUT_BARESOILT           %.4f    *       *
OUTVAR          OUT_SURF_TEMP           %.4f    *       *
OUTVAR          OUT_AIR_TEMP            %.4f    *       *
OUTVAR          OUT_ALBEDO              %.4f    *       *
OUTVAR          OUT_SWE                 %.4f    *       *
OUTVAR          OUT_SNOW_CANOPY         %.4f    *       *
OUTVAR          OUT_SURFSTOR            %.4f    *       *
OUTVAR          OUT_WDEW                %.6f    *       *
"""

subTemplate = """OUTFILE         sub     6
OUTVAR          OUT_ROOTMOIST   %.4f    *       *
OUTVAR          OUT_SOIL_MOIST  %.4f    *       *
OUTVAR          OUT_SOIL_TEMP   %.4f    *       *
OUTVAR          OUT_SMLIQFRAC   %.4f    *       *
OUTVAR          OUT_SMFROZFRAC  %.4f    *       *
OUTVAR          OUT_SOIL_WET    %.4f    *       *
"""

evaTemplate = """OUTFILE         eva     8
OUTVAR          OUT_EVAP_CANOP  %.6f    *       *
OUTVAR          OUT_TRANSP_VEG  %.6f    *       *
OUTVAR          OUT_EVAP_BARE   %.6f    *       *
OUTVAR          OUT_LAKE_EVAP   %.6f    *       *
OUTVAR          OUT_WDEW        %.4f    *       *
OUTVAR          OUT_SUB_SNOW    %.6f    *       *
OUTVAR          OUT_SUB_CANOP   %.6f    *       *
OUTVAR          OUT_AERO_COND   %.4f    *       *
"""

cspTemplate = """OUTFILE         csp     5
OUTVAR          OUT_SNOW_COVER  %.4f    *       *
OUTVAR          OUT_FDEPTH      %.4f    *       *
OUTVAR          OUT_TDEPTH      %.4f    *       *
OUTVAR          OUT_SALBEDO     %.4f    *       *
OUTVAR          OUT_SNOW_DEPTH  %.4f    *       *
"""


def template(varlist):
    """Returns string of VIC output template."""
    out = "N_OUTFILES\t{0}\n".format(len(varlist))
    out += "\n"
    if "eb" in varlist:
        out += ebTemplate
    if "csp" in varlist:
        out += cspTemplate
    if "wb" in varlist:
        out += wbTemplate
    if "sur" in varlist:
        out += surTemplate
    if "sub" in varlist:
        out += subTemplate
    if "eva" in varlist:
        out += evaTemplate
    return out


def variableGroup(args):
    """Returns new list of variables by expanding variable that corresponds to group name."""
    groupvars = {'snow': ["swe", "salbedo", "snow_cover", "snow_depth"],
                 'drought': ["severity", "spi1", "spi3", "spi6", "spi12", "sri1", "sri3", "sri6", "sri12", "smdi", "dryspells"],
                 'soil': ["soil_moist", "soil_temp"],
                 'eb': ["net_short", "net_long", "latent", "sensible", "grnd_flux"],
                 'wb': ["rainf", "snowf", "evap", "runoff", "baseflow"]}
    for v in args:
        if v in groupvars:
            args.remove(v)
            for gv in groupvars[v]:
                if gv not in args:
                    args.append(gv)
    return args
