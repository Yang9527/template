#coding=utf-8

import os
import sys
import logging
import datetime
import subprocess
import inspect
from os.path import join

#------------set up environment-------------------------------------
CUR_DIR, CUR_SCRIPT = os.path.split(os.path.abspath(sys.argv[0]))
HOME_LOCAL = os.path.split(CUR_DIR)[0]
HOME_SRC = join(HOME_LOCAL, "src")
HOME_LIB = join(HOME_LOCAL, "lib")
HOME_DATA = join(HOME_LOCAL, "data")
HOME_CONF = join(HOME_LOCAL, "conf")
TODAY = datetime.date.today()
LOG_FILE = "{}/log/{}-{}.log".format(HOME_LOCAL, CUR_SCRIPT, TODAY)
logging.basicConfig(
        filename = LOG_FILE, level = logging.INFO,
        format = "%(asctime)s [%(levelname)s]: %(message)s")

sys.path.append(HOME_SRC)
from util import *

import argparse
import ConfigParser
config = ConfigParser.ConfigParser()
config.read(join(HOME_CONF, "conf.ini"))
config = ConfigUtil(config)
parser = argparse.ArgumentParser()
parser.add_argument("--job", dest="job", help="name of the function be evaluated.")
params = parser.parse_args()

if __name__ == "__main__":
    eval(params.job)()

