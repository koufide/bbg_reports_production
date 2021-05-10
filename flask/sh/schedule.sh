#!/usr/bin/env bash

clear
python --version
pip --version
#activate
source /var/www/html/bbg-reports/flask/venv3/bin/activate
# source /var/www/html/bbg-reports/flask/venv2/bin/activate
python --version
pip --version
# ---------------------------------------------------------------

python /var/www/html/bbg-reports/flask/api/schedule.py

# ---------------------------------------------------------------
deactivate
python --version
pip --version