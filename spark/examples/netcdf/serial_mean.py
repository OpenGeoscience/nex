from netCDF4 import Dataset
import numpy as np
import argparse
import time

def calculate_means(nc_file, variable, n_timesteps):
    f = Dataset(nc_file)
    v = f.variables[variable]
    timesteps = v.shape[0]


    means = []

    for x in xrange(0, timesteps, n_timesteps):
        start = x
        end =  x + n_timesteps
        if x + n_timesteps >= timesteps:
            end = timesteps

        means.append(np.mean(v[start:end], axis=0))

    return means

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--datafile_path', required=True)
    parser.add_argument('-p', '--parameter', required=True)
    parser.add_argument('-n', '--timesteps', required=True, type=int, help='Number of timesteps to average over')

    config = parser.parse_args()

    start = time.time()

    means = calculate_means(config.datafile_path, config.parameter, config.timesteps)

    for m in means:
        print(m[~m.mask])

    end = time.time()

    print "Time: %f" % (end - start)
