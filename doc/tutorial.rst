Tutorial
=================================

Assuming that you have :ref:`installed <installation>` RHEAS, you can follow this document for a tutorial on running the system.

First let's check that RHEAS is correctly installed

.. highlight:: bash

::

   ./bin/rheas -h

which should show a help message. In order to perform a simulation, we'll follow four steps:

* Ingest datasets to drive the models
* Run the VIC hydrology model
* Run the DSSAT agricultural model
* Post-process and visualize the results

Building the database
---------------------------------
RHEAS offers the ability to automatically ingest various :ref:`datasets <datasets>`, and we will demonstrate this by ingesting precipitation data from CHIRPS, air temperature and wind speed from the NCEP reanalysis dataset. A simple configuration file, which we will name `data.conf` controls the geographic area and the time period we want the data downloaded and ingested into the PostGIS database.

.. compound::

We start by defining a bounding box::

  [domain]
  minlat:
  maxlat:
  minlon:
  maxlon:

and then describing each dataset in its own section with a starting and ending date::

  [chirps]
  startdate: 2011-1-1
  enddate: 2011-12-31

  [ncep]
  startdate: 2011-1-1
  enddate: 2011-12-31

We can then run RHEAS in its updating mode::

  ./bin/rheas -v -u data.conf

Feel free to toggle the `-v` flag to decrease verbosity.
