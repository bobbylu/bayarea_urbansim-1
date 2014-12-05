#!/usr/bin/env python

import os
import subprocess
import sys
import time

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


# Run county attribute processing scripts in parallel.
# Needs more memory (~8GB) to work with large pandas objects simultaneously.
county_names = ['ala', 'cnc', 'mar', 'nap', 'scl', 'sfr', 'smt', 'sol', 'son']
county_processes = []
for name in county_names:
    path = os.path.join('counties', name + '.py')
    county_processes.append((name, run(path)))

# Wait for completion of county attribute processing and check for success.
# Repeatedly iterate over all processes to quickly raise an exception
# if a non-zero exit status is found.
succeeded = set()
while len(succeeded) < len(county_processes):
    for (name, process) in county_processes:
        if name in succeeded:
            continue
        retcode = process.poll()
        if retcode:
            raise RuntimeError("County loading of {} failed.".format(name))
        elif retcode == 0:
            succeeded.add(name)
    time.sleep(1)


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
