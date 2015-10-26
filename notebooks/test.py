from __future__ import print_function
import json
import time
import avro.schema
# from netCDF4 import Dataset, num2date

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

# ds = Dataset("/data/tmp/pr_day_BCSD_historical_r1i1p1_ACCESS1-0_1997.nc", 'r', format='NETCDF4')

col1 = intern('col1')
col2 = intern('col2')
col3 = intern('col3')
col4 = intern('col4')

python_schema = {
    "namespace": "com.kitware.nex_gddp",
    "type": "record",
    "name": "GDDP Record",
    "fields": [
        {"name": col1, "type": "int"},
        {"name": col2, "type": "int"},
        {"name": col3, "type": "int"},
        {"name": col4, "type": "int"}
#        {"name": "lat",  "type": "float"},
#        {"name": "lon", "type": "float"},
#        {"name": "time", "type": "int"},
     ]
}
schema = avro.schema.parse(json.dumps(python_schema))

def print_status(msg):
    print(msg, end="")
    print('\r' * len(msg), end="")

def traditional_avro(N):
    from avro.datafile import DataFileReader, DataFileWriter
    from avro.io import DatumWriter



    writer = DataFileWriter(open("traditional_avro_{}_ints.avro".format(N),
                             "w"), DatumWriter(), schema)
    try:
        INTERVAL=1
        import numpy as np
        t_start = time.time()
        t0 = time.time()
        nums = np.random.random_integers(0, 100, (N, 4))
        print("Generated data ({:.2f})".format(time.time() - t0))



        i = 0
        t0 = time.time()
        for item in nums:
            writer.append(dict(zip((col1, col2, col3, col4), item)))

            if (time.time() - t0) > INTERVAL:
                print_status("Completed {0:.2f}% ({1:.2f})".format(
                    (i / float(N)) * 100,
                    time.time() - t_start))

                t0 = time.time()
            i = i + 1
        print("\n")

        print("Finished ({:.2f})".format(time.time() - t_start))
        return (N, time.time() - t_start)

    except Exception, e:
        raise e

    finally:
        writer.close()


def fastavro_avro(N):
    from fastavro import writer
    import numpy as np

    INTERVAL=1

    t_start = time.time()
    t0 = time.time()7
    nums = np.random.random_integers(0, 100, (N, 4))
    print("Generated data ({:.2f})".format(time.time() - t0))

    t0 = time.time()
    data = [dict(zip((col1, col2, col3, col4), item)) for item in nums]
    print("Transformed data ({:.2f})".format(time.time() - t0))

    with open("fast_avro_{}_ints.avro".format(N), "wb") as out:
        writer(out, python_schema, data)

    print("Finished ({:.2f})".format(time.time() - t_start))


if __name__ == "__main__":
#    traditional_avro(1000000)  #=> (1000000, 38.15)
    fastavro_avro(100000000)
