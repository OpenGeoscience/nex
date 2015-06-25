import argparse
import json
from netCDF4 import Dataset

def degrees_east_to_longitude(degrees_east):
    if degrees_east < 180:
        return degrees_east

    return degrees_east - 360

def convert(data_path, variable, timestep):
    data = Dataset(data_path)
    variable = data.variables[variable]
    shape = variable[timestep].shape

#    print '%f, %f' % (data.variables['lon'][0], data.variables['lat'][0])
#    print '%f, %f' % (data.variables['lon'][data.variables['lon'].size-1], data.variables['lat'][data.variables['lat'].size-1])

    contour_data = {
        'gridWidth': shape[0],
        'gridHeight': shape[1],
        'x0': degrees_east_to_longitude(data.variables['lon'][0]),
        'y0': data.variables['lat'][0],
        'dx': data.variables['lon'][1] - data.variables['lon'][0],
        'dy': data.variables['lat'][1] - data.variables['lat'][0],
        'values': variable[timestep].reshape(variable[timestep].size).tolist()
    }

    return json.dumps(contour_data, indent=2)
    #with open('/home/cjh/work/source/geojs/dist/data/out.json', 'w') as fp:
    #    json.dump(contour_data, fp, indent=2)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--data_path',  required=True)
    parser.add_argument('-p', '--parameter', required=True)
    parser.add_argument('-n', '--timestep', required=True, type=int)

    config = parser.parse_args()

    print convert(config.data_path, config.parameter, config.timestep)
