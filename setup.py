#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name="rheas",
    version="0.2",
    url="http://jpl.nasa.gov",
    license="MIT",
    description="Regional Hydrologic Extremes Assessment System",
    author="Kostas Andreadis'",
    packages=find_packages('src'),
    package_dir={'': 'src'},
    entry_points={'console_scripts': ['rheas=rheas:run']},
    install_requires=['setuptools'],
)
