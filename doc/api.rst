Customizing RHEAS
=================================

Initializing and updating the database
------------------------------------------

The RHEAS database needs to be populated with a variety of datasets that can be fetched automatically. A configuration file can be used to control and define which datasets are downloaded and imported in the PostGIS database. The configuration file follows the `INI <http://en.wikipedia.org/wiki/INI_file>`_ format, with each section corresponding to a dataset.

Unless the entire spatial extent of the datasets needs to be ingested into the database, a ``domain`` section should be created in the configuration file describing the bounding box for the domain of interest.

* ``minlat``: the minimum latitude of the bounding box
* ``maxlat``: the maximum latitude of the bounding box
* ``minlon``: the minimum longitude of the bounding box
* ``maxlon``: the maximum longitude of the bounding box

If the dataset is being fetched for the first time, a starting date needs to be provided within each dataset section while an ending date can be optionally provided.

* ``startdate``: start date of data fetched (format is year-month-day)
* ``enddate``: the last date of data to be fetched

If the dataset already exists in the database, then RHEAS will only download data after the last date available in the database (unless this is bypassed by the ``startdate`` keyword).

An example configuration file (named ``data.conf``) that will download TRMM, CHIRPS and IRI datasets is given below. 

.. compound::

   ::

     [domain]
     minlat: -2
     maxlat: -2
     minlon: 30
     maxlon: 34

     [trmm]

     [iri]
     startdate: 2000-2-1
     enddate: 2000-3-1

     [chirps]
     startdate: 2014-1-3


Since no option is provided under the ``trmm`` section, RHEAS will download data from the latest date available in the database to today.

After the configuration file has been created, the database can be initialized/updated by calling RHEAS as

.. highlight:: bash

::

./bin/rheas -u data.conf

where ``data.conf`` is the name of the configuration file.

  
Using a custom database
------------------------------------------

Assuming that you have created (using RHEAS or not) a PostGIS database (named ``customdb`` here as an example) that contains the necessary schemas and tables, it can be used to perform nowcast and forecast simulations by

.. highlight:: bash

::

./bin/rheas -d customdb nowcast.conf

where ``nowcast.conf`` is the RHEAS configuration file.


Writing custom scripts with the RHEAS API
--------------------------------------------

RHEAS exposes most of its functions within each module, allowing for a relatively simple API to be used to further customize it.

In the first example, we will assume that we want to run a deterministic VIC simulation but use custom meteorological data (from Numpy arrays). We first initialize a VIC object

.. highlight:: python

::

   import vic
   model = vic.VIC(".", "customdb", 0.25, 2015, 1, 1, 2015, 3, 31, "customname")

and then write the VIC global and soil parameter files

.. highlight:: python

::

   model.writeParamFile()
   model.writeSoilFile("basin.shp")

where ``basin.shp`` is a shapefile that describes our basin. Assuming that we have Numpy arrays containing the data for precipitation (``prec``), air temperature (``tmax`` and ``tmin``) and wind speed (``wind``) we can then write out the forcings for VIC, run the model and save the output into the database

.. highlight:: python

::

   model.writeForcings(prec, tmax, tmin, wind)
   model.run(vicexe)
   model.save("db", ["runoff"])


In a second example of using the RHEAS API, we will assume that we have a customized version of the DSSAT model (as an executable) that needs an additional line written in its configuration file. In order to achieve that, we will `decorate <https://wiki.python.org/moin/PythonDecorators>`_ the corresponding DSSAT class method and replace one of its parameters. We begin by initializing the DSSAT object

.. highlight:: python

::

   model = dssat.DSSAT("customdb", "customname", 0.25, 2015, 1, 1, 2015, 3, 31, 40, vicoptions, "basin.shp", True)

and then decorate the function ``writeConfigFile`` to change its behavior

.. highlight:: python

::

   def addLineToConfig(func):
       def wrapper(*args, **kwargs):
           fname = func(args, kwargs)
	   with open(fname, 'a') as fout:
	        fout.write("Additional line with parameters")
	return wrapper
	
   model.writeConfigFile = addLineToConfig(model.writeConfigFile)

Finally, we run the customized DSSAT model

.. highlight:: python

::

   model.run("dssat_new.exe")

