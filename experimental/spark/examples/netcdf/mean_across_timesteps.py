"""
 This script calculating means for a parameter at a given location across all
 timesteps, partitioned using lat, lons.

 Requires a parameter of the form (time, lat, lon)

 Requires more driver memory:

 spark-submit --driver-memory 4g ...

"""

from pyspark import SparkContext, SparkFiles, SparkConf
from netCDF4 import Dataset
import numpy as np
import argparse
import functools
from itertools import izip
import sys
import math

parser = argparse.ArgumentParser()
parser.add_argument('-m', '--master_url', required=True)
parser.add_argument('-d', '--datafile_path', required=True)
parser.add_argument('-p', '--parameter', required=True)
config = parser.parse_args()

# Build up the context, using the master URL
sc = SparkContext(config.master_url, 'global_mean')

data = Dataset(config.datafile_path)
pr = data.variables[config.parameter]

# Get the number of timesteps
num_timesteps = data.variables['time'].size

# Get number of locations per timestep
shape = pr[0].shape
num_locs = pr[0].size

data.close()

# Now partition locations across the cluster
locations = sc.parallelize(range(0, num_locs), 100)

# Function to load timestep
def load_locations(locations):

    data = Dataset(config.datafile_path)
    pr = data.variables[config.parameter]
    # Need to cover to list so we can get size
    locations = list(locations)
    # Setup list for values
    values = [np.ma.empty(num_timesteps) for x in xrange(len(locations))]

    for t in range(0, num_timesteps):
        step = pr[t]
        for i in range(0, len(locations)):
            x = locations[i]
            lat = (x % pr.shape[1]) - 1
            lon = (x / pr.shape[1]) - 1

            values[i][t] = step[lat][lon]

    return values

# Function to calculate mean for a given location across timestep
def mean(location_timesteps):
    return np.mean(location_timesteps)

# Load data across cluster
location_timesteps = locations.mapPartitions(load_locations)


# Now calculate the mean
means = location_timesteps.map(mean)

# collect the result
means = means.collect()

# convert to numpy and reshape
means = np.ma.asarray(means).reshape(shape)

# print out a sample of unmasked data
count = 0
for x in np.nditer(means):
    if not math.isnan(x):
        count +=1

        if count > 20:
            break

        print x


