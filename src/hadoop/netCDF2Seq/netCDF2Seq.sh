#!/bin/bash

BASE_DIR=${1:-/data/tmp}
HDFS_ROOT=${cat /opt/hadoop/2.7.1/etc/hadoop/core-site.xml | grep "hdfs://" | cut -f2 -d">" | cut -f1 -d"<"}

for NC_NAME in $(ls $BASE_DIR); do
	BASE_NAME=${NC_NAME%.nc}

	time java -jar /home/ubuntu/netCDF2Seq-1.0-SNAPSHOT.jar \
	          file://$BASE_DIR/$NC_NAME \
	          $HDFS_ROOT/user/nex/$BASE_NAME.seq

	echo "Finished with /user/nex/$BASE_NAME.seq"
done
