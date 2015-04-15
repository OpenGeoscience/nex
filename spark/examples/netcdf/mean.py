"""
 This script calculating means for each timestep for a netCDF file. The partitioning
 is done using timesteps. Note: For a file like this with only 60 timesteps this
 is pretty inefficient.

"""

from pyspark import SparkContext
from netCDF4 import Dataset
import numpy as np

# Build up the context, using the master URL
sc = SparkContext('spark://ulex:7077', 'mean')
data_path = '/media/bitbucket/pr_amon_BCSD_rcp26_r1i1p1_CONUS_bcc-csm1-1_202101-202512.nc'
data = Dataset(data_path)
pr = data.variables['pr']

# Get the number of timesteps
num_timesteps = data.variables['time'].size

data.close()

# Now partition timesteps across the cluster
timesteps = sc.parallelize(range(0, num_timesteps), 30)

# Function to load timestep
def load_timestep(timestep):
    data = Dataset(data_path)
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

