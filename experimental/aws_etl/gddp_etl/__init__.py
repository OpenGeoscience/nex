from __future__ import absolute_import
from celery import Celery
import tempfile
import os
import requests
import logging
import subprocess
import shutil

CONVERSION_JAR="/public/nex/src/parquet/target/scala-2.11/parquet-assembly-1.0.jar"

HADOOP_BIN="/opt/hadoop/2.7.1/bin/hadoop"
HADOOP_DATA_DIR="/home/ubuntu/"

app = Celery('example', broker='amqp://guest@172.31.38.99//')

app.conf.update(
    CELERY_TASK_RESULT_EXPIRES=300,
    CELERY_SEND_EVENTS=True,
    CELERY_SEND_TASK_SENT_EVENT=True,
    CELERY_TASK_SERIALIZER="json",
    CELERYD_TASK_TIME_LIMIT=1800,
    CELERYD_TASK_SOFT_TIME_LIMIT=1800
)


def build_url(opts):
    return ("http://nasanex.s3.amazonaws.com/NEX-GDDP/BCSD/{scenario}/day/atmos/{variable}/r1i1p1"
            "/v1.0/{variable}_day_BCSD_{scenario}_r1i1p1_{model}_{year}.nc").format(**opts)

def build_filename(opts):
    return "day_BCSD_{scenario}_r1i1p1_{model}_{year}.parquet".format(**opts)

def build_range(scenarios=None, models=None, years=None):

    if scenarios is None:
        scenarios = ["rcp45", "rcp85"]

    if models is None:
        models =  ["ACCESS1-0",
                   "bcc-csm1-1",
                   "BNU-ESM",
                   "CanESM2",
                   "CCSM4",
                   "CESM1-BGC",
                   "CNRM-CM5",
                   "CSIRO-Mk3-6-0",
                   "GFDL-CM3",
                   "GFDL-ESM2G",
                   "GFDL-ESM2M",
                   "inmcm4",
                   "IPSL-CM5A-LR",
                   "IPSL-CM5A-MR",
                   "MIROC5",
                   "MIROC-ESM",
                   "MIROC-ESM-CHEM",
                   "MPI-ESM-LR",
                   "MPI-ESM-MR",
                   "MRI-CGCM3",
                   "NorESM1-M"]

    if years is None:
        years = range(1950, 2101)

    for year in years:
        _scenarios = ["historical"] if year <= 2005 else scenarios
        for model in models:
            for scenario in _scenarios:

                # If we're in a year less than 2005 we're really in the
                # historical scenario


                # These models do not have values for 2100
                if (model == "bcc-csm1-1" or model == "MIROC5") and year == 2100:
                    continue

                yield (build_url({"model": model, "scenario": scenario, "year": year, "variable": "pr" }),
                       build_url({"model": model, "scenario": scenario, "year": year, "variable": "tasmin" }),
                       build_url({"model": model, "scenario": scenario, "year": year, "variable": "tasmax" }),
                       build_filename({"model": model, "scenario": scenario, "year": year}))



@app.task(name="example.etl")
def etl(pr_url, tasmin_url, tasmax_url, out_file):

    # Set up Logging
    logger = logging.getLogger('gddp.etl')
    logger.setLevel(logging.INFO)

    # Currently just log to console
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)

    formatter = logging.Formatter('%(asctime)s | %(name)s | %(levelname)s | %(message)s')
    ch.setFormatter(formatter)


    logger.addHandler(ch)

    # Create a temporary directory
    directory = tempfile.mkdtemp()

    # Download the files
    for url in [pr_url, tasmin_url, tasmax_url]:
        local_file = os.path.join(directory, os.path.basename(url))

        logger.info("Downloading {} to {}".format(url, local_file))

        r = requests.get(url, stream=True)

        with open(local_file, 'wb') as fh:
            for chunk in r.iter_content(chunk_size=1024 * 1024 * 100):
                if chunk:
                    fh.write(chunk)

    logger.info("Finished Download files")

    cmd = ["java", "-jar", CONVERSION_JAR,
           os.path.join(directory, os.path.basename(pr_url)),
           os.path.join(directory, os.path.basename(tasmin_url)),
           os.path.join(directory, os.path.basename(tasmax_url)),
           os.path.join(directory, out_file)]


    logger.info("Running \"{}\" to convert to parquet".format(" ".join(cmd)))

    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    for line in iter(proc.stdout.readline, b''):
        logger.info(line)


    logger.info("Finished writing to {}".format(cmd[-1]))


    # Ensure directory exists
    cmd = [HADOOP_BIN, "fs", "-mkdir", HADOOP_DATA_DIR]
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()

    if stderr != '':
        logger.warn(stderr)

    # Copy the file
    cmd = [HADOOP_BIN, "fs", "-copyFromLocal", "-f",  os.path.join(directory, out_file), os.path.join(HADOOP_DATA_DIR, out_file)]
    logger.info("Running {} to load parquet into HDFS".format(" ".join(cmd)))

    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()

    if stderr != '':
        logger.warn(stderr)

    logger.info("Finished loading parquet into HDFS at {}".format(os.path.join(HADOOP_DATA_DIR, out_file)))

    # Delete the local copy - should do some kind of checksum here
    shutil.rmtree(directory, ignore_errors=True)


@app.task
def fib(x):
    if x < 2:
        return 1
    return fib(x-1) + fib(x -2 )


if __name__ == "__main__":
    etl.delay(
        'http://nasanex.s3.amazonaws.com/NEX-GDDP/BCSD/rcp45/day/atmos/pr/r1i1p1/v1.0/pr_day_BCSD_rcp45_r1i1p1_ACCESS1-0_2006.nc',
        'http://nasanex.s3.amazonaws.com/NEX-GDDP/BCSD/rcp45/day/atmos/tasmin/r1i1p1/v1.0/tasmin_day_BCSD_rcp45_r1i1p1_ACCESS1-0_2006.nc',
        'http://nasanex.s3.amazonaws.com/NEX-GDDP/BCSD/rcp45/day/atmos/tasmax/r1i1p1/v1.0/tasmax_day_BCSD_rcp45_r1i1p1_ACCESS1-0_2006.nc',
        'day_BCSD_rcp45_r1i1p1_ACCESS1-0_2006.parquet')
