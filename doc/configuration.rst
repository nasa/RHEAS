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
* ``save``: a comma-separated list of variables to be saved from VIC. See the :ref:`list of outputs <output>` for further information.

* ``observations``: a comma-separated list of the observations to be assimilated into VIC. Any of the datasets with ``AS`` mode outlined in the :ref:`database table <database>` can be used with their table name (without the schema, e.g. ``grace``)
* ``update``: the date or frequency when assimilation should be performed. Valid options for the assimilation frequency are: ``daily``, ``weekly``, and ``monthly``. If this option is not set, assimilation is performed whenever the observation is available during the simulation period. When performing a forecast simulation, this option is not taken into account and assimilation is performed at the forecast initialization date


DSSAT options
----------------------------------
The options for the DSSAT model include:

* ``shapefile``: a shapefile contains the areas (e.g. administrative boundaries) for which DSSAt will be run (*required*)
* ``ensemble size``: the size of the ensemble to be used (*required*)







