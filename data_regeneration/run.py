#!/usr/bin/env python

import os
import subprocess
import sys

from spandex import TableLoader


python = sys.executable
root_path = os.path.dirname(__file__)


def run(filename):
    """"Run Python file relative to script without blocking."""
    path = os.path.join(root_path, filename)
    return subprocess.Popen([python, path])


def check_run(filename):
    """Run Python file relative to script, block, assert exit code is zero."""
    path = os.path.join(root_path, filename)
    return subprocess.check_call([python, path])


print("PREPROCESSING: Loading shapefiles by county.")


# Load shapefile data inputs, fix invalid geometries, and reproject.
check_run('load.py')


print("PROCESSING: Loading parcel attributes by county.")


# Run county attribute processing scripts.
county_names = ['ala', 'cnc', 'mar', 'nap', 'scl', 'sfr', 'smt', 'sol', 'son']
for name in county_names:
    filename = os.path.join('counties', name + '.py')
    check_run(filename)


print("PROCESSING: Combining to create regional parcels table.")


# Join the county attributes and geometries and union the counties together.
loader = TableLoader()
sql_path = os.path.join(root_path, 'join_counties.sql')
with open(sql_path) as sql:
    with loader.database.cursor() as cur:
        cur.execute(sql.read())


print("POSTPROCESSING: Applying spatial operations.")


# Apply spatial operations.
check_run('spatialops.py')


print("SUMMARIZING: Generating data summaries.")


# Output summary CSV files by county and TAZ.
check_run('summaries.py')
