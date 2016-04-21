from __future__ import absolute_import
from celery import Celery
import tempfile
import os
import requests
import logging
import subprocess
import shutil
import socket
import pwd
import glob

CONVERSION_JAR="/public/nex/src/parquet/target/scala-2.11/parquet-assembly-1.0.jar"

HADOOP_BIN="/opt/hadoop/2.7.2/bin/hadoop"

with open(os.path.dirname(__file__) + "/.master", "r") as fh:
    master_hostname = fh.read().rstrip()


app = Celery('example',
             broker='amqp://guest@{}//'.format(master_hostname),
             backend='amqp://guest@{}//'.format(master_hostname))

# from kombu.common import Broadcast

app.conf.update(
    CELERY_TASK_RESULT_EXPIRES=300,
    CELERY_SEND_EVENTS=True,
    CELERY_SEND_TASK_SENT_EVENT=True,
    CELERY_TASK_SERIALIZER="json",
    CELERY_TASK_TIME_LIMIT=1800,
    CELERY_TASK_SOFT_TIME_LIMIT=1800,
    CELERY_TRACK_STARTED=True,
    CELERYD_PREFETCH_MULTIPLIER=1
)

# Set up Logging
logger = logging.getLogger('gddp.etl')
logger.setLevel(logging.INFO)

# Currently just log to console
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s | %(name)s | %(levelname)s | %(message)s')
ch.setFormatter(formatter)


sysh = logging.handlers.SysLogHandler(address=(master_hostname, 514))
sysh.setLevel(logging.INFO)
import socket

formatter = logging.Formatter(socket.gethostname() + ' | %(asctime)s | %(name)s | %(levelname)s | %(message)s')
sysh.setFormatter(formatter)


logger.addHandler(ch)
logger.addHandler(sysh)

def build_url(opts):
    return ("http://nasanex.s3.amazonaws.com/NEX-GDDP/BCSD/"
            "{scenario}/day/atmos/{variable}/r1i1p1"
            "/v1.0/{variable}_day_BCSD_{scenario}_"
            "r1i1p1_{model}_{year}.nc").format(**opts)


def build_filename(opts):
    return "day_BCSD_{scenario}_r1i1p1_{model}_{year}.parquet".format(**opts)


def build_range(prefix=None, scenarios=None, models=None, years=None):

    if prefix is None:
        prefix = lambda x: ""

    if scenarios is None:
        scenarios = ["rcp45", "rcp85"]

    if models is None:
        models = ["ACCESS1-0",
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
        # If we're in a year less than 2005 we're really in the
        # historical scenario
        _scenarios = ["historical"] if year <= 2005 else scenarios

        for model in models:
            for scenario in _scenarios:

                # These models do not have values for 2100
                if (model == "bcc-csm1-1" or model == "MIROC5") and \
                   year == 2100:
                    continue

                opts = {"model": model, "scenario": scenario, "year": year}

                yield (build_url({"model": model, "scenario": scenario,
                                  "year": year, "variable": "pr"}),
                       build_url({"model": model, "scenario": scenario,
                                  "year": year, "variable": "tasmin"}),
                       build_url({"model": model, "scenario": scenario,
                                  "year": year, "variable": "tasmax"}),
                       prefix(opts) + build_filename(opts))


def hadoop_copy_from_local(src, dest, overwrite=None, libjars=None):
    cmd = [HADOOP_BIN, "fs"]

    if libjars is not None:
        cmd.extend(["-libjars", libjars])

    cmd.append("-copyFromLocal")

    if overwrite is not None:
        cmd.append("-f")

    cmd.extend([src, dest])

    logger.info("Running {}".format(" ".join(cmd)))

    p = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()

    if stderr != '':
        logger.warn(stderr)

    logger.info("Finished loading parquet to {}".format(
        dest))


@app.task(bind=True, name="example.parquet_etl",
          default_retry_delay=30, max_retries=3)
def parquet_etl(self, s3_url, hdfs_url=None):
    try:
        directory = "/tmp/parquet_etl"
        try:
            os.makedirs(directory)
        except OSError:
            pass

        local_file = os.path.join(directory, os.path.basename(s3_url))
        logger.info("Downloading {} to {}".format(s3_url, local_file))

        r = requests.get(s3_url, stream=True)

        with open(local_file, 'wb') as fh:
            for chunk in r.iter_content(chunk_size=1024 * 1024 * 100):
                if chunk:
                    fh.write(chunk)

        logger.info("Finished Downloading file")
    except Exception as exc:
        raise self.retry(exc=exc)


@app.task(bind=True, name="example.etl",
          default_retry_delay=30, max_retries=3,
          acks_late=True)
def etl(self, pr_url, tasmin_url, tasmax_url, out_file,
        hdfs_url=None, s3_url=None, overwrite=True):
    try:
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

        logger.info("Running \"{}\" to convert to parquet".format(
            " ".join(cmd)))

        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)
        for line in iter(proc.stdout.readline, b''):
            logger.info(line)

        logger.info("Finished writing to {}".format(cmd[-1]))

        ret = []

        # Copy the file
        if hdfs_url is not None:
            # Ensure directory exists
            cmd = [HADOOP_BIN, "fs", "-mkdir", "-p", hdfs_url]
            p = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE)
            stdout, stderr = p.communicate()

            if stderr != '':
                logger.warn(stderr)

            # hadoop_copy_from_local here
            hadoop_copy_from_local(os.path.join(directory, out_file),
                                   os.path.join(hdfs_url, out_file),
                                   overwrite=overwrite)

            ret.append(os.path.join(hdfs_url, out_file))

        if s3_url is not None:
            libjars = ",".join(glob.glob(
                "/opt/hadoop/2.7.1/share/hadoop/tools/lib/*.jar"))

            hadoop_copy_from_local(os.path.join(directory, out_file),
                                   os.path.join(s3_url, out_file),
                                   overwrite=overwrite, libjars=libjars)

            ret.append(os.path.join(s3_url, out_file))

        # Delete the local copy - should do some kind of checksum here
        if hdfs_url is not None or s3_url is not None:
            shutil.rmtree(directory, ignore_errors=True)
            logger.info("Removed {}".format(directory))
        else:
            hostname = socket.gethostname()
            user = pwd.getpwuid(os.getuid()).pw_name
            ret.append("{}@{}:{}".format(user, hostname,
                                         os.path.join(directory, out_file)))

        logger.info("Wrote: {}".format(", ".join(ret)))
        return ret
    except Exception as exc:
        raise self.retry(exc=exc)


if __name__ == "__main__":
    etl.delay(
        'http://nasanex.s3.amazonaws.com/NEX-GDDP/BCSD/historical/day/atmos/pr/r1i1p1/v1.0/pr_day_BCSD_historical_r1i1p1_ACCESS1-0_1997.nc',
        'http://nasanex.s3.amazonaws.com/NEX-GDDP/BCSD/historical/day/atmos/tasmin/r1i1p1/v1.0/tasmin_day_BCSD_historical_r1i1p1_ACCESS1-0_1997.nc',
        'http://nasanex.s3.amazonaws.com/NEX-GDDP/BCSD/historical/day/atmos/tasmax/r1i1p1/v1.0/tasmax_day_BCSD_historical_r1i1p1_ACCESS1-0_1997.nc',
        'day_BCSD_historical_r1i1p1_ACCESS1-0_1997.parquet',
        hdfs_url="hdfs://localhost:9000/home/ubuntu/",
        overwrite=False)
