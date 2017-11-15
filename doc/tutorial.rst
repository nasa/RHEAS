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



