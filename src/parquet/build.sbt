name := "parquet"

version := "1.0"

scalaVersion := "2.11.7"

resolvers ++= Seq(
  "UNIDATA Releases" at "https://artifacts.unidata.ucar.edu/content/repositories/unidata-releases/",
  "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
  "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/"
)
val netcdf_version = "4.6.3"

libraryDependencies ++= Seq(
  "edu.ucar" % "cdm" % netcdf_version,
  "edu.ucar" % "netcdf4" % netcdf_version,

  "org.scalanlp" %% "breeze" % "0.12",
  "org.scalanlp" %% "breeze-natives" % "0.12",

  "org.apache.avro" % "avro" % "1.7.6",

  "com.twitter" % "parquet-avro" % "1.6.0"
)
