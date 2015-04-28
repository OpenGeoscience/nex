"""
 This script calculating means for a parameter at a given location across all
 timesteps, partitioned using lat, lons.

 Requires a parameter of the form (time, lat, lon)

 Requires more driver memory:

 spark-submit --driver-memory 10g ...

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
parser.add_argument('-n', '--timesteps', required=True, type=int, help='Number of timesteps to average over')

config = parser.parse_args()

spark_config = SparkConf();
spark_config.set('spark.akka.frameSize', 32)
spark_config.set('spark.executor.memory', '2g')
spark_config.set('spark.driver.maxResultSize', '4g')
spark_config.set('spark.shuffle.memoryFraction', 0.6)
spark_config.set('spark.serializer', 'org.apache.spark.serializer.KryoSerializer')

# Build up the context, using the master URL
sc = SparkContext(config.master_url, 'n_timesteps_mean', conf=spark_config)

data = Dataset(config.datafile_path)
pr = data.variables[config.parameter]

# Get the number of timesteps
num_timesteps = data.variables['time'].size

# Get number of locations per timestep
shape = pr[0].shape
num_grid_points = pr[0].size

data.close()

grid_points = range(0, num_grid_points)

# Break timesteps into n size chunks
timestep_chunks = []
for x in xrange(0, num_timesteps, config.timesteps):
    if x + config.timesteps < num_timesteps:
        timestep_chunks.append((x, x + config.timesteps))
    else:
        timestep_chunks.append((x, num_timesteps))

# Now partition locations across the cluster
grid_points = sc.parallelize(grid_points, 200)

# Function to process a set of locations for this partition
def load_locations(points):

    data = Dataset(config.datafile_path)
    pr = data.variables[config.parameter]
    # Need to cover to list so we can get size
    locations = list(points)
    # Setup list for values
    values = [[np.ma.empty(config.timesteps) for y in xrange(len(timestep_chunks))] for x in xrange(len(locations))]

    timestep_range_index = 0
    for timestep_range in timestep_chunks:
        (start, end) = timestep_range
        for t in range(start, end):
            step = pr[t]
            for i in range(0, len(locations)):
                x = locations[i]
                lat = (x % pr.shape[1]) - 1
                lon = (x / pr.shape[1]) - 1

                values[i][timestep_range_index][t-start] = step[lat][lon]
        timestep_range_index += 1

    return values

# Function to calculate mean for a given location across timesteps
def mean(location_timesteps_chunks):
    means = []
    for chunk in location_timesteps_chunks:
        means.append(np.mean(chunk))

    return means

# Load data across cluster
grid_timesteps = grid_points.mapPartitions(load_locations)

# Now calculate the means
means = grid_timesteps.map(mean)
means.cache()

# collect the results
means = means.collect()

# convert to numpy and reshape
means = np.ma.asarray(means) #.reshape(shape[0], shape[1], len(timestep_chunks))

# print out a sample of unmasked data
count = 0
for x in np.nditer(means):
    if not math.isnan(x):
        count +=1

        if count > 20:
            break

        print x


