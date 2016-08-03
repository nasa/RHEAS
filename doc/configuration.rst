Populating the configuration file
=================================

The configuration file follows the `INI <http://en.wikipedia.org/wiki/INI_file>`_ format to allow for simplicity. When running the RHEAS executable, the configuration file is the only position argument while the database name needs to be provided with the ``-d`` switch.

Running RHEAS with the help switch

.. highlight:: bash

::

./rheas -h

.. compound::

   produces the proper usage command ::

    usage: rheas.py [-h] [-d DB] [-u] config
   
    Runs RHEAS simulation.

    positional arguments:
    config      configuration file
 
    optional arguments:
    -h, --help  show this help message and exit
    -d DB       name of database to connect
    -u          update database

There are four possible sections for the configuration file:

* **nowcast**: nowcast simulation options
* **forecast**: forecast simulation options
* **vic**: VIC model options
* **dssat**: DSSAT model options

Each section needs to be given inside braces, e.g. ``[nowcast]``.


Nowcast options
----------------------------------
The available options for a nowcast simulation include:

* ``model``: the model(s) to be used for this simulation. Valid options include ``vic`` and ``dssat``; if both models are requested they need to be separated by a comma (*required*)
* ``startdate``: the start date of the simulation in the "year-month-day" format (*required*)
* ``enddate``: the start date of the simulation in the "year-month-day" format (*required*)
* ``name``: name of the simulation (*required*)
* ``basin``: path to a shapefile of the model domain. If not provided, the ``name`` option should correspond to a previously performed simulation
* ``resolution``: spatial resolution of the simulation (*required*)


Forecast options
----------------------------------
The available options for a forecast simulation include:

* ``model``: the model(s) to be used for this simulation. Valid options include ``vic`` and ``dssat``; if both models are requested they need to be separated by a comma (*required*)
* ``startdate``: the start date of the simulation in the "year-month-day" format (*required*)
* ``enddate``: the start date of the simulation in the "year-month-day" format (*required*)
* ``name``: name of the simulation (*required*)
* ``basin``: path to a shapefile of the model domain. If not provided, the ``name`` option should correspond to a previously performed simulation
* ``resolution``: spatial resolution of the simulation (*required*)
* ``ensemble size``: the size of the forecast ensemble (*required*)
* ``method``: method to use to generate the meteorological forcings for VIC (*required*). The options that have been implemented include:

  * ``esp``: use the Ensemble Streamflow Prediction approach that randomly resamples the climatology
  * ``iri``: resample climatology based on the probabilities in the IRI meteorological forecasts


VIC options
----------------------------------
The options for the VIC model include:

* ``precip``: dataset to use for precipitation forcing (*required*)
* ``temperature``: dataset to use for maximum and minimum temperature forcing (*required*)
* ``wind``: dataset to use for wind speed forcing (*required*)
* ``lai``: dataset to use for leaf area index forcing
* ``save state``: directory where VIC model state file is saved in
* ``save to``: option for saving output variables. Can be one of

  * ``db``: save output to database
  * path to copy raw VIC output files to

* ``initialize``: whether to initialize the model from a previously saved state file (can be given as ``on/off``, ``true/false`` or ``yes/no``)
* ``save``: a comma-separated list of variables to be saved from VIC. The variable names can be:

  * ``net_long``:  net downward longwave flux [W/m2] 
  * ``net_short``:  net downward shortwave flux [W/m2
  * ``snow_cover``:  fractional area of snow cover [fraction] 
  * ``salbedo``:  snow pack albedo [fraction] 
  * ``snow_depth``:  depth of snow pack [cm] 
  * ``tdepth``:  depth of thawing fronts [cm] for each thawing front
  * ``fdepth``:  depth of freezing fronts [cm] for each freezing front
  * ``rootmoist``:  root zone soil moisture  [mm] 
  * ``smfrozfrac``:  fraction of soil moisture (by mass) that is ice, for each soil layer 
  * ``smliqfrac``:  fraction of soil moisture (by mass) that is liquid, for each soil layer 
  * ``snow_canopy``:  snow interception storage in canopy  [mm] 
  * ``soil_moist``:  soil total moisture content  [mm] for each soil layer 
  * ``soil_wet``:  vertical average of (soil moisture - wilting point)/(maximum soil moisture - wilting point) [mm/mm] 
  * ``surfstor``:  storage of liquid water and ice (not snow) on surface (ponding) [mm] 
  * ``swe``:  snow water equivalent in snow pack (including vegetation-intercepted snow)  [mm] 
  * ``wdew``:  total moisture interception storage in canopy [mm] 
  * ``baseflow``:  baseflow out of the bottom layer  [mm]
  * ``evap``:  total net evaporation [mm]
  * ``evap_bare``:  net evaporation from bare soil [mm]
  * ``evap_canop``:  net evaporation from canopy interception [mm]
  * ``prec``:  incoming precipitation [mm]
  * ``rainf``:  rainfall  [mm]
  * ``refreeze``:  refreezing of water in the snow  [mm]
  * ``runoff``:  surface runoff [mm]
  * ``snow_melt``:  snow melt  [mm]
  * ``snowf``:  snowfall  [mm]
  * ``transp_veg``:  net transpiration from vegetation [mm]
  * ``albedo``:  average surface albedo [fraction] 
  * ``baresoilt``:  bare soil surface temperature [C]
  * ``snow_surf_temp``:  snow surface temperature [C]
  * ``soil_temp``:  soil temperature [C] for each soil layer 
  * ``surf_temp``:  average surface temperature [C]
  * ``vegt``:  average vegetation canopy temperature [C]
  * ``advection``:  advected energy [W/m2] 
  * ``grnd_flux``:  net heat flux into ground [W/m2] 
  * ``latent``:  net upward latent heat flux [W/m2] 
  * ``melt_energy``:  energy of fusion (melting) in snowpack [W/m2] 
  * ``rfrz_energy``:  net energy used to refreeze liquid water in snowpack [W/m2] 
  * ``sensible``:  net upward sensible heat flux [W/m2] 
  * ``aero_cond``:  "scene" aerodynamic conductance [m/s]
  * ``air_temp``:  air temperature [C]
  * ``longwave``:  incoming longwave [W/m2] 
  * ``shortwave``:  incoming shortwave [W/m2] 

* ``observations``: a comma-separated list of the observations to be assimilated into VIC. Any of the datasets with ``AS`` mode outlined in the :ref:`database table <database>` can be used with their table name (without the schema, e.g. ``grace``)
* ``update``: the date or frequency when assimilation should be performed. Valid options for the assimilation frequency are: ``daily``, ``weekly``, and ``monthly``. If this option is not set, assimilation is performed whenever the observation is available during the simulation period. When performing a forecast simulation, this option is not taken into account and assimilation is performed at the forecast initialization date


DSSAT options
----------------------------------
The options for the DSSAT model include:

* ``shapefile``: a shapefile contains the areas (e.g. administrative boundaries) for which DSSAT will be run (*required*)
* ``ensemble size``: the size of the ensemble to be used (*optional*)
* ``assimilate``: flag indicating whether to assimilate soil moisture (``sm``), LAI (``lai``) observations or both (*optional*)







