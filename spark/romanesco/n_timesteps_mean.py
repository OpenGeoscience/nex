"""
 This script calculating means for a parameter at a given location across n timesteps,
 partitioned using lat, lons.

 Requires a parameter of the form (time, lat, lon)

 Requires more driver memory:

 spark-submit --driver-memory 10g ...

"""

from netCDF4 import Dataset
import numpy as np
import argparse
import functools
from itertools import izip
import sys
import math
import time
import os

def _copy_variable(target_file, source_file, variable):
    src_var = source_file.variables[variable]
    target_var = target_file.createVariable(variable, src_var.datatype, src_var.dimensions)
    target_var.setncatts({k: src_var.getncattr(k) for k in src_var.ncattrs()})
    target_var[:] = src_var[:]

def write(path, source, variable, data):
    output = Dataset(path, 'w')
    output.createDimension('lat', len(source.dimensions['lat']) if not source.dimensions['lat'].isunlimited() else None)
    output.createDimension('lon', len(source.dimensions['lon']) if not source.dimensions['lon'].isunlimited() else None)
    output.createDimension('time', len(source.dimensions['time']) if not source.dimensions['time'].isunlimited() else None)
    _copy_variable(output, source, 'lat')
    _copy_variable(output, source, 'lon')

    if type(data) == list:
        data_type = data[0].dtype
    else:
        data_type = data.dtype

    output.createVariable(variable, data_type, ('time', 'lat', 'lon'))

    print data.shape
    print output.variables[variable][:].shape

    if type(data) == list:
        for i in xrange(len(data)):
            output.variables[variable][i] = data[i]
    else:
        output.variables[variable][:] = data
    output.close()

start = time.time()

data = Dataset(datafile_path)
pr = data.variables[parameter]

# Get the number of timesteps
num_timesteps = data.variables['time'].size

# Get number of locations per timestep
shape = pr[0].shape
num_grid_points = pr[0].size

# Break timesteps into n size chunks
timestep_chunks = []
for x in xrange(0, num_timesteps, timesteps):
    if x + timesteps < num_timesteps:
        timestep_chunks.append((x, x + timesteps))
    else:
        timestep_chunks.append((x, num_timesteps))

grid_chunk_size = grid_chunk_size

# Break locations into chunks
grid_chunks = []
for lat in xrange(0, shape[0], grid_chunk_size):
    for lon in xrange(0, shape[1], grid_chunk_size):
        grid_chunks.append((lat, lon))

print 'Grid chunks: %d' % len(grid_chunks)

# Function to process a set of locations for this partition
def calculate_means(grid_chunk):

    data = Dataset(datafile_path)
    pr = data.variables[parameter]

    (lat, lon) = grid_chunk

    values = []
    for timestep_range in timestep_chunks:
        (start_timesteps, end_timesteps) = timestep_range

        mean = np.mean(pr[start_timesteps:end_timesteps,
                          lat:lat+grid_chunk_size,
                          lon:lon+grid_chunk_size], axis=0)
        values.append(mean)

    return values

# parallelize the grid
grid_chunks = sc.parallelize(grid_chunks, partitions)

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


if output_path:
    path = os.path.join(output_path, os.path.basename(datafile_path).replace('.nc', '_means.nc'))
    write(path, data, '%s_mean' % parameter, timestep_means)
else:
    for m in timestep_means:
        print(m[~m.mask])

end = time.time()

runtime = end - start

