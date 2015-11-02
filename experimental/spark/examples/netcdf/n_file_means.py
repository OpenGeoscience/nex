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
from serial_mean import calculate_means
from common import write
import time
import glob
import os
import functools

def process_file(config, file_path):
    nc_file = Dataset(file_path)
    means = calculate_means(nc_file, config.parameter, config.timesteps)
    path = os.path.join(config.output_path, os.path.basename(file_path).replace('.nc', '_means.nc'))
    write(path, nc_file, '%s_mean' % config.parameter, means)

def process_files(config, sc):
    files = glob.glob(os.path.join(config.datadir_path, '*.nc'))

    func = functools.partial(process_file, config)
    # parallelize the file paths
    files = sc.parallelize(files, config.partitions)

    # Now calculate means
    files.foreach(func)

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-m', '--master_url', required=True)
    parser.add_argument('-d', '--datadir_path', required=True)
    parser.add_argument('-o', '--output_path', required=True)
    parser.add_argument('-p', '--parameter', required=True)
    parser.add_argument('-n', '--timesteps', required=True, type=int, help='Number of timesteps to average over')
    parser.add_argument('-s', '--partitions', default=10, type=int)

    config = parser.parse_args()

    spark_config = SparkConf();
    spark_config.set('spark.akka.frameSize', 32)
    spark_config.set('spark.executor.memory', '16g')
    spark_config.set('spark.driver.maxResultSize', '4g')
    spark_config.set('spark.shuffle.memoryFraction', 0.6)
    spark_config.set('spark.serializer', 'org.apache.spark.serializer.KryoSerializer')
    spark_config.set('spark.kryoserializer.buffer.max.mb', 1024)

    # Build up the context, using the master URL
    sc = SparkContext(config.master_url, 'n_file_means', conf=spark_config)

    sc.addPyFile(os.path.join(os.path.dirname(__file__),  'serial_mean.py'))
    sc.addPyFile(os.path.join(os.path.dirname(__file__),  'common.py'))

    start = time.time()

    process_files(config, sc)

    end = time.time()

    print "Time: %f" % (end - start)











