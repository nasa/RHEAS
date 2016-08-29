#!/data/rheas/RHEAS/bin/rheaspy
# CHANGE THE INTERPRETER TO LOCAL INSTALLATION

import rheas
import logging
from datetime import datetime


def main():
    dt = datetime.today()
    logging.basicConfig(filename="/data/rheas/RHEAS/log/update.{0}".format(dt.strftime("%Y%m%d")), level=logging.INFO, format='%(levelname)s: %(message)s')
    rheas.update("rheas", "/data/rheas/RHEAS/data.conf")


if __name__ == '__main__':
    main()
