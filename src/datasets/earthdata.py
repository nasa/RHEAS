""" Definition for RHEAS Earthdata module.

.. module:: earthdata
   :synopsis: Definition of the Earthdata module

.. moduleauthor:: Kostas Andreadis <kandread@jpl.nasa.gov>

"""

import requests
import tempfile
import re
from lxml import html


def download(url, filepattern):
    """Download data files from Earthdata search."""
    session = requests.session()
    resp_dir = session.get(url)
    links = html.fromstring(resp_dir.content).xpath('//a/@href')
    matches = [re.match(filepattern, link) for link in links]
    filenames = [m.group(0) for m in matches if m]
    # make sure list has unique filenames
    filenames = list(set(filenames))
    tmppath = tempfile.mkdtemp()
    filename = None
    for filename in filenames:
        resp_file = session.get("{0}/{1}".format(url, filename))
        with open("{0}/{1}".format(tmppath, filename), 'wb') as fout:
            for chunk in resp_file:
                fout.write(chunk)
    return tmppath, filename
