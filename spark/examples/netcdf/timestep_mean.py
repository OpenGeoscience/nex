"""
 This script calculating means for each timestep for a netCDF file. The partitioning
 is done using timesteps.

 Note: For a file like this with only 60 timesteps this
 is pretty inefficient.

-a and -s can be specified to download the dataset direct from s3
-l will use addFile to copy the data file to every node

 Note: Also if you provide your AWS credential they will be copied around which
 may not be secure!!

"""

from pyspark import SparkContext, SparkFiles, SparkConf
from netCDF4 import Dataset
import numpy as np
import argparse


parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('-a', '--access_key')
parser.add_argument('-s', '--secret_access_key')
parser.add_argument('-l', '--copy_local', action='store_true')

config = parser.parse_args()

download = False;

spark_config = None
if config.access_key and config.secret_access_key:
    download = True
    spark_config = SparkConf()
    spark_config.setExecutorEnv('AWS_ACCESS_KEY_ID', config.access_key)
    spark_config.setExecutorEnv('AWS_SECRET_ACCESS_KEY ', config.secret_access_key)


# Build up the context, using the master URL
sc = SparkContext('spark://ulex:7077', 'mean', conf=spark_config)
local_data_path = '/media/bitbucket/pr_amon_BCSD_rcp26_r1i1p1_CONUS_bcc-csm1-1_202101-202512.nc'
data_path = local_data_path
data_url = 'https://nasanex.s3.amazonaws.com/NEX-DCP30/BCSD/rcp26/mon/atmos/pr/r1i1p1/v1.0/CONUS/pr_amon_BCSD_rcp26_r1i1p1_CONUS_bcc-csm1-1_202101-202512.nc'

if download:
    data_path = data_url

# Download the file onto each node
if download or config.copy_local:
    sc.addFile(data_path)

# Still need to open dataset on master node to get number of timesteps. For
# some reason the master node doesn't seem to be able to access the downloaded
# version, this may be a bug in addFile(...)
data = Dataset(local_data_path)
pr = data.variables['pr']

# Get the number of timesteps
num_timesteps = data.variables['time'].size

data.close()

# Now partition timesteps across the cluster
timesteps = sc.parallelize(range(0, num_timesteps), 30)

# Function to load timestep
def load_timestep(timestep):
    path = data_path
    if download or config.copy_local:
        path = SparkFiles.get('pr_amon_BCSD_rcp26_r1i1p1_CONUS_bcc-csm1-1_202101-202512.nc')
    data = Dataset(path)
    pr = data.variables['pr']
    step = pr[timestep]
    # Return valid values
    return (timestep, step[~step.mask])

# Function to calcualte mean for a given timestep
def timestep_mean(timestep_data):
    return (timestep_data[0], np.mean(timestep_data[1]))

# First load each timestep
timestep_data = timesteps.map(load_timestep)

# Now calculate mean
means = timestep_data.map(timestep_mean).collect()

print 'Means for each timestep: %s' % str(map(lambda a : a[1], means))

