#!/bin/bash

# Setup the hdfs directory structure to work with the tutorial
# https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html
HADOOP_CMD=/opt/hadoop/2.7.1/bin/hadoop

echo "Hello World Bye World" > /tmp/file01
echo "Hello Hadoop Goodbye Hadoop" > /tmp/file02

$HADOOP_CMD fs -mkdir -p /user/joe/wordcount/input/
$HADOOP_CMD fs -put /tmp/file01 /tmp/file02 /user/joe/wordcount/input/
