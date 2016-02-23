name := "parquet"

version := "1.0"

scalaVersion := "2.11.7"

resolvers ++= Seq(
  "UNIDATA Releases" at "https://artifacts.unidata.ucar.edu/content/repositories/unidata-releases/",
  "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
  "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/"
)
val netcdf_version = "4.6.3"
val parquet_version = "1.8.1"

libraryDependencies ++= Seq(
  "edu.ucar" % "cdm" % netcdf_version,
  "edu.ucar" % "netcdf4" % netcdf_version,

  "org.scalanlp" %% "breeze" % "0.12",
  "org.scalanlp" %% "breeze-natives" % "0.12",

  "org.apache.avro" % "avro" % "1.7.6",

  "org.apache.hadoop" % "hadoop-common" % "2.7.1",
  "org.apache.hadoop" % "hadoop-hdfs" % "2.7.1",

  "org.apache.parquet" % "parquet-common" % parquet_version,
  "org.apache.parquet" % "parquet-encoding" % parquet_version,
  "org.apache.parquet" % "parquet-column" % parquet_version,
  "org.apache.parquet" % "parquet-hadoop" % parquet_version,

  //"com.twitter" % "parquet-avro" % "1.6.0"
  "org.apache.parquet" % "parquet-avro" % parquet_version
)

