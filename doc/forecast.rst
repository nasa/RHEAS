Running a forecast
=================================

As any RHEAS simulation, we begin by creating a configuration file. In your favorite text editor create a new file, let's assume that its name is ``forecast.conf``. The first section contains the simulation options and has a ``forecast`` header:

::

[forecast]

.. compound::

   Let's assume that we are requesting both a hydrologic and agricultural simulation. In that case, we need to set both models (VIC and DSSAT) as an option ::

     [forecast]
     model: vic, dssat

.. compound::

   We then set the starting and ending dates of the forecast ::

    [forecast]
    model: vic, dssat
    startdate: 2001-4-1
    enddate: 2001-6-30

.. compound::

   Let's also assume that we have a shapefile of the study domain (the sample data contain such a file in the ``data/tests`` directory). Moreover, let's set the name of the simulation (that name will be useful to retrieve the output from the database). ::

    [forecast]
    model: vic, dssat
    startdate: 2001-4-1
    enddate: 2001-6-30
    basin: data/tests/basin.shp
    name: basin

.. compound::

   The model spatial resolution is next, here set as 0.25 degrees (~25 km). ::

    [forecast]
    model: vic, dssat
    startdate: 2001-4-1
    enddate: 2001-6-30
    basin: data/tests/basin.shp
    name: basin
    resolution: 0.25

.. compound::

   In order to capture the uncertainty in the forecast, we perform an ensemble simulation and set the ensemble size to 10. RHEAS will run the VIC ensemble in parallel (using multiple threads), thus reducing computation time. The ensemble simulation also requires a method to generate the meteorological forcings ensemble (currently, model uncertainty due to parameter errors is not implemented). Available methods are the Ensemble Streamflow Prediction (ESP, resampling the climatology); resampling from the IRI meteorological forecasts; bias-correcting and downscaling the CFSv2 meteorological forecasts. ::

    [forecast]
    model: vic, dssat
    startdate: 2001-4-1
    enddate: 2001-6-30
    basin: data/tests/basin.shp
    name: basin
    resolution: 0.25    
    ensemble size: 10
    method: esp

These are all the necessary options to define our forecast simulation. Then we need to define the parameters for the two models (VIC and DSSAT).

.. compound::

   We begin with the ``vic`` section ::

    [vic]

.. compound::
   
   The generation of the meteorological forecast ensemble requires a base dataset for each of the required variables. Therefore, we need to define the source for precipitation, temperature and wind speed ::

    [vic]
    precip: chirps
    temperature: ncep
    wind: ncep

.. compound::

   We can opt to initialize the ensemble of VIC models from a a state file saved in the database, by randomly selecting a model initial condition from climatology, or by starting a 1-year model run. All these options do not need to be set by the user apart from ::

    [vic]
    precip: chirps
    temperature: ncep
    wind: ncep
    initialize: yes

   If RHEAS cannot find previous model states in the database, it will by default run a 1-year simulation for each ensemble member.

.. compound::

   We choose the model output, specifically net shortwave radiation and soil moisture, to the database ::

    [vic]
    precip: chirps
    temperature: ncep
    wind: ncep
    initialize: yes
    save to: db
    save: net_short, soil_moist

.. compound::

   The DSSAT section ``dssat`` requires fewer options than VIC, with the domain shapefile being the first ::

    [dssat]
    shapefile: data/tests/basin.shp

.. compound::

   and the ensemble size being another parameter that can optionally be set. The forecasts from VIC are randomly paired with each of the DSSAT ensemble members in order to capture uncertainty both in the DSSAT model parameters and the hydroclimatological forcings. Similarly to a nowcast, assimilation cna be enabled/disabled with an option. ::
 
    [dssat]
    shapefile: data/tests/basin.shp
    ensemble size: 50
    assimilate: no

Finally, let's run the system (inside the ``rheas`` directory)

.. highlight:: bash

::

./bin/rheas forecast.conf
