name := "monthly_means"

version := "1.0"

scalaVersion := "2.11.4"

// resolvers += Resolver.url("UNIDATA Releases", url("https://artifacts.unidata.ucar.edu/content/repositories/unidata-releases"))
resolvers += "UNIDATA Release" at "https://artifacts.unidata.ucar.edu/content/repositories/unidata-releases"

libraryDependencies ++= Seq(
  "edu.ucar" % "cdm" % "4.6.3",
  "org.apache.spark" % "spark-core_2.10" % "1.5.1" % "provided",
  "org.apache.hadoop" % "hadoop-client" % "2.7.1" % "provided",
  "org.slf4j" % "slf4j-jdk14" % "1.7.5"
)
