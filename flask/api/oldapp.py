# coding: utf-8

import sys
from flask import Flask, jsonify, request
from flask_cors import CORS, cross_origin

# import numpy as np
import pandas as pd

import json
import os

from xml.dom import minidom
import configparser
from pathlib import Path
from datetime import datetime

from mesmodules.ConnexionMysql import ConnexionMysql 
from mesmodules.ConnexionPostgres import ConnexionPostgres
from mesmodules.ConnexionOracle import ConnexionOracle
from mesmodules.ConnexionSqlServer import ConnexionSqlServer


if sys.version_info < (3, 4):
    raise Exception("This application must be run under Python 3.4 or later.")


app = Flask(__name__)

# cors = CORS(app)

@app.route('/')
def home():
    return 'Python Flask - 3'
    # return sys.path

# if __name__ == "__main__":
#     app.run(host="192.168.56.220", port=7002, debug=True)