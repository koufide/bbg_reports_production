#!/usr/bin/env python

import sys

# import site

def verifier():
    import os
    print("PYTHONPATH:", os.environ.get('PYTHONPATH'))
    print("PATH:", os.environ.get('PATH'))


if sys.version_info < (3, 4):
    raise Exception("This application must be run under Python 3.4 or later.")

# site.addsitedir("/usr/local/venvs/venv1/lib/python3.9/site-packages")
# site.addsitedir("/var/www/html/bbg-reports/flask/api","/usr/local/venvs/venv2/lib/python3.9/site-packages")

# sys.path.insert(0, "/usr/local/venvs/venv2")
sys.path.insert(0, "/var/www/html/bbg-reports/flask/api")
sys.path.insert(1, "/var/www/html/bbg-reports/flask/venv2")
sys.path.insert(2, "/var/www/html/bbg-reports/flask/venv2/lib/python3.6/site-packages")
# sys.path.insert(3, "/usr/local/lib/python3.9/site-packages")



print("KF PREF : {} " .format(repr(sys.prefix)))
print("KF PATH : {}  " .format(repr(sys.path)))

verifier()

from app import app as application


