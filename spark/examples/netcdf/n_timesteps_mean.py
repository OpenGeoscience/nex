"""
 This script calculating means for a parameter at a given location across n timesteps,
 partitioned using lat, lons.

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
from serial_mean import calculate_means as serial_means
from common import write
import time
import os

parser = argparse.ArgumentParser()
parser.add_argument('-m', '--master_url', required=True)
parser.add_argument('-d', '--datafile_path', required=True)
parser.add_argument('-p', '--parameter', required=True)
parser.add_argument('-n', '--timesteps', required=True, type=int, help='Number of timesteps to average over')
parser.add_argument('-v', '--validate', action='store_true')
parser.add_argument('-s', '--partitions', default=8, type=int)
parser.add_argument('-c', '--grid_chunk_size', default=2000, type=int)
parser.add_argument('-o', '--output_path')

config = parser.parse_args()

spark_config = SparkConf();
spark_config.set('spark.akka.frameSize', 32)
spark_config.set('spark.executor.memory', '4g')
spark_config.set('spark.driver.maxResultSize', '4g')
spark_config.set('spark.shuffle.memoryFraction', 0.6)
spark_config.set('spark.serializer', 'org.apache.spark.serializer.KryoSerializer')
spark_config.set('spark.kryoserializer.buffer.max.mb', 1024)

# Build up the context, using the master URL
sc = SparkContext(config.master_url, 'n_timesteps_mean', conf=spark_config)

sc.addPyFile(os.path.join(os.path.dirname(__file__),  'common.py'))

start = time.time()

data = Dataset(config.datafile_path)
pr = data.variables[config.parameter]

# Get the number of timesteps
num_timesteps = data.variables['time'].size

# Get number of locations per timestep
shape = pr[0].shape
num_grid_points = pr[0].size

# Break timesteps into n size chunks
timestep_chunks = []
for x in xrange(0, num_timesteps, config.timesteps):
    if x + config.timesteps < num_timesteps:
        timestep_chunks.append((x, x + config.timesteps))
    else:
        timestep_chunks.append((x, num_timesteps))

grid_chunk_size = config.grid_chunk_size

# Break locations into chunks
grid_chunks = []
# for x in xrange(0, num_grid_points, grid_chunk_size):
#     if x + grid_chunk_size < num_grid_points:
#         grid_chunks.append((x, x + grid_chunk_size))
#     else:
#         grid_chunks.append((x, num_grid_points))
for lat in xrange(0, shape[0], grid_chunk_size):
    for lon in xrange(0, shape[1], grid_chunk_size):
        grid_chunks.append((lat, lon))

print 'Grid chunks: %d' % len(grid_chunks)

# Function to process a set of locations for this partition
def calculate_means(grid_chunk):

    data = Dataset(config.datafile_path)
    pr = data.variables[config.parameter]

    (lat, lon) = grid_chunk

    values = []
    for timestep_range in timestep_chunks:
        (start_timesteps, end_timesteps) = timestep_range

        mean = np.mean(pr[start_timesteps:end_timesteps,
                          lat:lat+grid_chunk_size,
                          lon:lon+grid_chunk_size], axis=0)
        values.append(mean)

    return values

# Validate parallel result against serial
def validate(means, datafile_path, parameter, timesteps):
    serial = serial_means(datafile_path, parameter, timesteps)

    valid = True
    for i in range(len(timestep_chunks)):
        v1 = serial[i][~serial[i].mask]
        v2 = timestep_means[i][~timestep_means[i].mask]
        valid &= np.allclose(v1, v2)

    if valid:
        print "Results match serial versions"
    else:
        print "Results DO NOT match serial versions!"

# parallelize the grid
grid_chunks = sc.parallelize(grid_chunks, config.partitions)

# Now calculate means
means = grid_chunks.map(calculate_means)

# collect the results
means = means.collect()

# Now combine the chunks
timestep_means = [np.ma.empty(shape) for x in range(len(timestep_chunks))]

i = 0
for lat in xrange(0, shape[0], grid_chunk_size):
    for lon in xrange(0, shape[1], grid_chunk_size):
        for j in range(len(timestep_chunks)):
            chunk = means[i][j]
            timestep_means[j][lat:lat+chunk.shape[0], lon:lon+chunk.shape[1]] = chunk

        i += 1


if config.output_path:
    path = os.path.join(config.output_path, os.path.basename(config.datafile_path).replace('.nc', '_means.nc'))
    write(path, data, '%s_mean' % config.parameter, timestep_means)
else:
    for m in timestep_means:
        print(m[~m.mask])

end = time.time()

print "Time: %f" % (end - start)

if config.validate:
    validate(timestep_means, config.datafile_path, config.parameter, config.timesteps)

