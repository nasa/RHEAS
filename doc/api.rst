Customizing RHEAS
=================================

Initializing and updating the database
--------------------------------

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

After the configuration file has been created, the database can be initialized/updated by calling RHEAS as

.. highlight:: bash

::

./bin/rheas -u data.conf

where ``data.conf`` is the name of the configuration file.

  

