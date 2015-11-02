"""
 This script does a simple MapReduce operation to calculate the total flight
 delay in 2008.

 The data set taken from here:

 http://stat-computing.org/dataexpo/2009/the-data.html
"""
import sys
from pyspark import SparkContext

# The data file, needs to be available to all node in the cluster
data_file = '/media/bitbucket/2008.csv'

# Build up the context, using the master URL
sc = SparkContext('spark://ulex:7077', 'flight delay')

# Load the data, returns a RDD
data = sc.textFile(data_file)

# Remove header
header = data.first()
data = data.filter(lambda line: line != header)

# Cache the data to keep in memory
data.cache()

# The map phase splits the lines and extract the field we want
def map(line):

    parts = line.split(',')

    try:
        return int(parts[14])
    except ValueError:
        return 0

# Now perform the MapReduce
print 'Total delays in 2008: %d hours' % (data.map(map).reduce(lambda a, b: a+b)/60)

