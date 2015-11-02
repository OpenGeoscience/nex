# Modified heavily from:
# https://github.com/gettyimages/docker-spark/blob/master/Dockerfile

FROM ubuntu:14.04
MAINTAINER Chris Kotfila

COPY opt/ /opt

RUN apt-get update \
  && apt-get -y install openjdk-7-jdk

# JAVA
ENV JAVA_HOME /usr/lib/jvm/java-7-openjdk-amd64
ENV PATH $PATH:$JAVA_HOME/bin

# PYTHON
RUN apt-get -y install libpython-dev python-pip python-numpy

# netCDF
RUN apt-get -y install libhdf5-dev
RUN apt-get -y install libnetcdf-dev

RUN pip install netCDF4


# SPARK
ENV SPARK_VERSION 1.4.1
ENV HADOOP_VERSION 2.4
ENV SPARK_PACKAGE $SPARK_VERSION-bin-hadoop$HADOOP_VERSION
ENV SPARK_HOME /opt/spark
ENV PATH $PATH:$SPARK_HOME/bin

CMD /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master
