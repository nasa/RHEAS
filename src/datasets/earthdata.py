""" Definition for RHEAS Earthdata module.

.. module:: earthdata
   :synopsis: Definition of the Earthdata module

.. moduleauthor:: Kostas Andreadis <kandread@jpl.nasa.gov>

"""

import requests
import tempfile
import re


username = "nasarheas"
password = "rheas@Earthdata1"


def download(url, filepattern):
    """Download data files from Earthdata search."""
    session = requests.session()
    data = {'login': username, 'password': password}
    resp_auth = session.post("http://urs.earthdata.nasa.gov", data=data)
    resp_dir = session.get(url)
    p = re.compile(filepattern)
    matches = p.search(resp_dir.text)
    tmppath = tempfile.mkdtemp()
    filename = None
    if matches:
        filename = str(matches.group(0))
        resp_file = session.get("{0}/{1}".format(url, filename))
        with open("{0}/{1}".format(tmppath, filename), 'wb') as fout:
            for chunk in resp_file:
                fout.write(chunk)
    return tmppath, filename
