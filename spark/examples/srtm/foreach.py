"""
 This script demonstrates using a foreach function and two accumulator to calculate the
 average elevation in North America using SRTM data.

"""

import sys
from pyspark import SparkContext
import StringIO
import zipfile
import re
import os
import sys
import numpy as np

srtm_dtype = np.dtype('>i2')
filename_regex = re.compile('([NSEW]\d+[NSEW]\d+).*')

# The data directory, needs to be available to all node in the cluster
data_files = '/media/bitbucket/srtm/version2_1/SRTM3/North_America'

# Build up the context, using the master URL
sc = SparkContext('spark://ulex:7077', 'srtm')

# Now load all the zip files into a RDD
data = sc.binaryFiles(data_files)

# The two accumulators are used to collect values across the cluster
num_samples_acc = sc.accumulator(0)
sum_acc = sc.accumulator(0)

# Function to array
def read_array(data):
    hgt_2darray = np.flipud(np.fromstring(data, dtype=srtm_dtype).reshape(1201, 1201))

    return hgt_2darray

# Function to process a HGT file
def process_file(file):
    (name, content) = file

    filename = os.path.basename(name)
    srtm_name = filename.split('.')[0]
    match = filename_regex.match(srtm_name)

    # Skip anything that doesn't match
    if not match:
        return

    hgt_file = '%s.hgt' % match.group(1)

    stream = StringIO.StringIO(content)
    try:
        with zipfile.ZipFile(stream, 'r') as zipfd:
            hgt_data = zipfd.read(hgt_file)
            data = read_array(hgt_data)
            samples = 0
            sum = 0
            for x in np.nditer(data):
                if x != -32768:
                    samples += 1
                    sum += x

            # Add the the local results to the global accumulators
            num_samples_acc.add(samples)
            sum_acc.add(sum)
    except zipfile.BadZipfile:
        # Skip anything thats not a zip
        pass

# For each file do the processing
data.foreach(process_file)

# Calculate the average using the global values
print 'Average elevation in North America: %d meters' % (sum_acc.value / num_samples_acc.value)
