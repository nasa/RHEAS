Installation
=================================

Requirements
--------------------------------
The requirements for installing RHEAS include

* `Python <https://www.python.org>`_ and a number of python packages

   * `Numpy <http://www.numpy.org>`_
   * `NetCDF4 <https://github.com/Unidata/netcdf4-python>`_
   * `GDAL <http://www.gdal.org>`_
   * `Scipy <http://www.scipy.org>`_

* `GCC compiler <https://gcc.gnu.org>`_
* `Automake <https://www.gnu.org/software/automake/>`_
* `PostgreSQL <http://www.postgresql.org>`_ server with the `PostGIS <http://postgis.net>`_ extension
* `VIC <http://hydro.washington.edu/Lettenmaier/Models/VIC/>`_ model executable
* `DSSAT <http://dssat.net>`_ model executable

Thankfully most of these requirements are automatically installed using `Buildout <http://www.buildout.org/en/latest/>`_, a Python-based build system for creating, assembling and deploying applications from multiple parts.


Installing on Linux and MacOS
--------------------------------
In order to compile RHEAS, we need a C compiler and the Make utilities, as well as some other software packages. Depending on the Linux distribution these can be installed

.. highlight:: bash

::

 yum install make automake gcc gcc-c++ python-devel

for RPM-based distros (such as RedHat, Fedora, Centos)

.. highlight:: bash

::

 sudo aptitude update && sudo aptitude -y upgrade  
 sudo aptitude install git build-essential python-numpy python-scipy python-gdal python-argparse python-dateutil libgdal-dev libproj-dev libxslt-dev libreadline-dev cython python-pandas python-pip python-psycopg2
 sudo dpkg --add-architecture i386
 sudo aptitude update
 sudo aptitude -y install wine winetricks
 sudo pip install netCDF4

for DEB-based distros (such as Ubuntu, Debian, Mint)

If you're on a MacOS, the easiest way to install the dependencies is by using the `Homebrew <http://brew.sh>`_ package manager. Follow the instructions to install Homebrew and then run

.. highlight:: bash

::

   brew update
   brew install python readline gdal netcdf wine winetricks sfcgal
   pip install numpy scipy gdal argparse py-dateutil

We then clone and uncompress the software archive

.. highlight:: bash

::

   git clone https://github.com/nasa/RHEAS.git
   cd RHEAS


and then run the ``buildout`` script. Before that, we boostrap the build by running

.. highlight:: bash

::

 python bootstrap.py

After that we can install everything by running

.. highlight:: bash

::

 ./bin/buildout

The ``buildout`` script will install the ``PostgresQL`` database along with the ``PostGIS`` extension, and create a database for the user. The script will also install some additional Python modules, and build the ``VIC`` hydrology model.

After the script finishes you should have a ``rheas`` executable in your ``bin`` directory!


Installing on Windows
--------------------------------

TBC

