Running a nowcast
=================================

We begin by creating the necessary RHEAS configuration file. In your favorite text editor create a new file, let's assume that its name is ``nowcast.conf``. The first section contains the simulation options and has a ``nowcast`` header:

::

[nowcast]

.. compound::

   Let's assume that we are requesting both a hydrologic and agricultural simulation. In that case, we need to set both models (VIC and DSSAT) as an option ::

     [nowcast]
     model: vic, dssat

.. compound::

   We then set the starting and ending dates of the nowcast ::

    [nowcast]
    model: vic, dssat
    startdate: 2003-1-1
    enddate: 2003-3-31

.. compound::

   Let's also assume that we have a shapefile of the study domain (the sample data contain such a file in the ``data/tests`` directory). Moreover, let's set the name of the simulation (that name will be useful to retrieve the output from the database). ::

    [nowcast]
    model: vic, dssat
    startdate: 2003-1-1
    enddate: 2003-3-31
    basin: data/tests/basin.shp
    name: basin

.. compound::

   The model spatial resolution is next, here set as 0.25 degrees (~25 km). ::

    [nowcast]
    model: vic, dssat
    startdate: 2003-1-1
    enddate: 2003-3-31
    basin: data/tests/basin.shp
    name: basin
    resolution: 0.25

These are all the necessary options to define our forecast simulation. Then we need to define the parameters for the two models (VIC and DSSAT).

.. compound::

   We begin with the ``vic`` section ::

    [vic]

.. compound::
   
   VIC requires precipitation, temperature and wind speed at a minimum, which we define below ::

    [vic]
    precip: chirps
    temperature: ncep
    wind: ncep

.. compound::
   
   Setting the meteorological forcings this way will perform a deterministic nowcast simulation. Alternatively, a stochastic simulation can be performed by either requesting multiple datasets for precipitation or explicitly requesting an ensemble simulation ::

    [vic]
    precip: chirps, trmm
    temperature: ncep
    wind: ncep
    ensemble size: 3

.. compound::

   We can opt to initialize the VIC from a a state file saved in the database or an explicitly file name ::

    [vic]
    precip: chirps
    temperature: ncep
    wind: ncep
    initialize: yes
    initial state: state/vic.state_20030101

   If the option ``initial state`` is not set, RHEAS will look into the database for a statefile that is closest to the requested start date.

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

   The DSSAT section ``dssat`` requires fewer options than VIC, with the shapefile being the first ::

    [dssat]
    shapefile: data/tests/basin.shp

.. compound::

   and the ensemble size being an optional parameter. Each of the DSSAT ensemble members is forced by the same VIC output retaining the uncertainty only in the DSSAT model parameters. There's also an option to enable or disable the assimilation of soil moisture and LAI data (enabled by default) ::
 
    [dssat]
    shapefile: 
    ensemble size: 50
    assimilate: on

Finally, let's run the system (inside the ``rheas`` directory)

.. highlight:: bash

::

./bin/rheas nowcast.conf
