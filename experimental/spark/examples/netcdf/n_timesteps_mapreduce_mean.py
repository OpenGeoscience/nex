"""
 This script calculating means for a parameter at a given location n
 timesteps, partitioned using timestep chunk.

 Requires a parameter of the form (time, lat, lon)

 Requires more driver memory:

 spark-submit --driver-memory 10g ...

"""

from pyspark import SparkContext, SparkConf
from netCDF4 import Dataset
import numpy as np
import argparse
from common import write
import time
import os

# Function to calculate partial sum for a timestep chunk
def calculate_partial_sums(timestep_chunk):
    print timestep_chunk
    data = Dataset(config.datafile_path)
    parameter = data.variables[config.parameter]

    (mean_timestep, range) = timestep_chunk
    sum = np.ma.zeros(shape)
    count = np.zeros(shape)

    for i in xrange(*range):
        sum += parameter[i, 0:shape[0], 0:shape[1]]
        # We need to keep track of valid values todo the division
        mask = parameter[i, 0:shape[0], 0:shape[1]].mask
        count += (~mask).astype(int)

    return (mean_timestep, [sum, count])

# Reduction function to reduce two partial sums
def reduce_partial_sums(p1, p2):
    return (p1[0]+p2[0],
            p1[1]+p2[1])

# Calculate mean
def calculate_mean(parts):
    (timestep_period, (sum, count)) = parts

    return (timestep_period, sum / count)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-m', '--master_url', required=True)
    parser.add_argument('-d', '--datafile_path', required=True)
    parser.add_argument('-p', '--parameter', required=True)
    parser.add_argument('-n', '--timesteps', required=True, type=int, help='Number of timesteps to average over')
    parser.add_argument('-b', '--timestep_chunk_size', required=True, type=int)
    parser.add_argument('-s', '--partitions', default=None, type=int)
    parser.add_argument('-o', '--output_path', required=True)

    config = parser.parse_args()

    spark_config = SparkConf();
    spark_config.set('spark.akka.frameSize', 32)
    spark_config.set('spark.executor.memory', '16g')
    spark_config.set('spark.driver.maxResultSize', '4g')
    spark_config.set('spark.shuffle.memoryFraction', 0.6)
    spark_config.set('spark.serializer', 'org.apache.spark.serializer.KryoSerializer')
    spark_config.set('spark.kryoserializer.buffer.max.mb', 1024)

    # Build up the context, using the master URL
    sc = SparkContext(config.master_url, 'n_timesteps_mapreduce_mean', conf=spark_config)

    sc.addPyFile(os.path.join(os.path.dirname(__file__),  'common.py'))

    start_time = time.time()

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
        start = x
        if x + config.timesteps < num_timesteps:
            end = x + config.timesteps
        else:
            end = num_timesteps

        for y in xrange(start, end, config.timestep_chunk_size):
            chunk_start = y
            if y + config.timestep_chunk_size < end:
                chunk_end = y + config.timestep_chunk_size
            else:
                chunk_end = end

            timestep_chunks.append((x, (chunk_start, chunk_end)))

    # parallelize the timestep chunks
    chunks = sc.parallelize(timestep_chunks, config.partitions)

    # Now calculate means
    chunks = chunks.map(calculate_partial_sums)

    # Now reduce
    sums = chunks.reduceByKey(reduce_partial_sums)

    # Now finally calculate the means
    timestep_means = sums.map(calculate_mean)

    timestep_means = sorted(timestep_means.collect())

    if config.output_path:
        timestep_means = np.ma.asarray([x[1] for x in timestep_means])
        path = os.path.join(config.output_path, os.path.basename(config.datafile_path).replace('.nc', '_means.nc'))
        write(path, data, '%s_mean' % config.parameter, timestep_means)
    else:
        for (_, m) in timestep_means:
            print(m[~m.mask])

    end_time = time.time()

    print "Time: %f" % (end_time - start_time)







