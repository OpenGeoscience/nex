from __future__ import print_function
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
import json
import time
import sys
from netCDF4 import Dataset, num2date

# Too Slow
# def netcdf_items(var):
#     ds = var._grp
#     
#     def to_unix_timestamp(i):
#         return time.mktime(num2date(ds.variables['time'][i], 
#                                     ds.variables['time'].units ).timetuple())
# 
#     times, lats, lons = var.shape
#     for t in xrange(times):
#         for lt in xrange(lats):
#             for ln in xrange(lons):
#                 yield (to_unix_timestamp(t), 
#                        ds['lat'][lt], ds['lon'][ln], var[t,lt,ln])
# 
# def netcdf_dicts(var):
#     for time, lat, lon, v in netcdf_items(var):
#         yield {"time": int(time), "lat": float(lat),
#                "lon": float(lon), "variable": float(v)}


python_schema = {
    "namespace": "com.kitware.nex_gddp",
    "type": "record",
    "name": "GDDP Record",
    "fields": [
#        {"name": "key", "type": "string"},
        {"name": "variable", "type": "float"},
        {"name": "lat",  "type": "float"},
        {"name": "lon", "type": "float"},
        {"name": "time", "type": "int"},
     ]
}

schema = avro.schema.parse(json.dumps(python_schema))

writer = DataFileWriter(open("pr_day_BCSD_historical_r1i1p1_ACCESS1-0_1997.avro", 
                             "w"), DatumWriter(), schema)


ds = Dataset("/data/tmp/pr_day_BCSD_historical_r1i1p1_ACCESS1-0_1997.nc", 'r', format='NETCDF4')

try:
    VAR='pr'
    N_ROWS = reduce(lambda x, y: x*y, ds[VAR].shape)
    
    t0 = time.time()
    t_start = time.time()
    i = 0
    for item in netcdf_dicts(ds[VAR]):
#        writer.append(item)
        i = i + 1
except Exception, e:
    raise e
finally:
    print(i)
    writer.close()
