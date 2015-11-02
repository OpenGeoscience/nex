"""
 This script demostrates using SparkSQL to query a data set.

 The data set taken from here:

 http://stat-computing.org/dataexpo/2009/the-data.html
"""
import sys
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row

# The data file, needs to be available to all node in the cluster
data_file = '/media/bitbucket/2008.csv'

# Build up the context, using the master URL
sc = SparkContext('spark://ulex:7077', 'flight')

# Load the data, returns a RDD
data = sc.textFile(data_file)

# Remove header
header = data.first()
data = data.filter(lambda line: line != header)
data.cache()

# Create the SQL context from our SparkContext
sql_context = SQLContext(sc)

# Use a map to extract out a column of interests
parts = data.map(lambda line: line.split(','))
parts = parts.filter(lambda p: p[14].isdigit())

# Map the data to rows
flight_data = parts.map(lambda p: Row(year=int(p[0]), month=int(p[1]), day=int(p[2]), delay=int(p[14])))

# Create the data frame
flight_data_frame = sql_context.inferSchema(flight_data)

# Register the table table to use in queries
flight_data_frame.registerTempTable('flight')

# Now perform a query
print 'Number of flights delayed over 4 hours: %d' % (sql_context.sql('SELECT * FROM flight WHERE delay > 240').count())

