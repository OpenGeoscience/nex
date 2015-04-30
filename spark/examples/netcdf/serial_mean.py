from netCDF4 import Dataset
import numpy as np
import argparse
import time
import os
import glob
from common import write

def calculate_means(f, variable, n_timesteps):
    v = f.variables[variable]
    timesteps = v.shape[0]

    means = np.ma.empty([timesteps/n_timesteps, v.shape[1], v.shape[2]])

    i = 0
    for x in xrange(0, timesteps, n_timesteps):
        start = x
        end =  x + n_timesteps
        if x + n_timesteps >= timesteps:
            end = timesteps

        means[i] = np.mean(v[start:end], axis=0)
        i += 1

    return means

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--data_path', required=True)
    parser.add_argument('-p', '--parameter', required=True)
    parser.add_argument('-n', '--timesteps', required=True, type=int, help='Number of timesteps to average over')
    parser.add_argument('-o', '--output_path')

    config = parser.parse_args()

    if os.path.isdir(config.data_path):
        files = glob.glob(os.path.join(config.data_path, '*.nc'))
    else:
        files =  [config.data_path]

    start = time.time()

    for f in files:
        print f
        nc_file = Dataset(f)
        means = calculate_means(nc_file, config.parameter, config.timesteps)

        if config.output_path:
            path = os.path.join(config.output_path, os.path.basename(f).replace('.nc', '_means.nc'))
            write(path, nc_file, '%s_mean' % config.parameter, means)
        else:
            for m in means:
                print(m[~m.mask])

    end = time.time()

    print "Time: %f" % (end - start)
