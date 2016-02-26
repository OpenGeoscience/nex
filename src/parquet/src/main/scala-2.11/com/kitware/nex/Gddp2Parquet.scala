package com.kitware.nex


import org.apache.hadoop.fs.Path

import breeze.linalg._
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.parquet.avro.AvroParquetWriter
import ucar.nc2.units.DateUnit
import ucar.nc2.{NetcdfFile, Variable}
import scala.util.Try


import collection.JavaConversions._

/**
  * Created by kotfic on 2/4/16.
  */
object Gddp2Parquet {

  def readNetCDF(path: String) : Try[NetcdfFile] = Try(NetcdfFile.open(path))

  /*
  // See: https://github.com/rustandruin/test-scala-netcdf/blob/master/src/main/scala/netcdfTestCode.scala#L111-L123
  def toBreezeMatrix[T](nc: NetcdfFile, varName: String) : Tuple2[DenseMatrix[T], DenseMatrix[Boolean]] = {
    var variable = nc.findVariable(varName)
    var time = variable.getDimension(0).getLength
    var lat = variable.getDimension(1).getLength
    var lon = variable.getDimension(2).getLength
    var fillValue = variable.findAttribute("_FillValue").getNumericValue.asInstanceOf[T]
    var breezeMat = (new DenseVector(variable.read.copyTo1DJavaArray.asInstanceOf[Array[T]])).toDenseMatrix.reshape(time, lat, lon)
    var maskMat = breezeMat.copy.mapValues(_ == fillValue)
    return (breezeMat, maskMat)
  }
  */

  def getDatum(schema: Schema,
               lat: Float, lon: Float, time: Long, model: String,
            pr: Float, tasmin: Float, tasmax: Float): GenericRecord ={
    val datum : GenericRecord = new GenericData.Record(schema)
    datum.put("lat", lat)
    datum.put("lon", lon)
    datum.put("time", time)
    datum.put("model", model)
    datum.put("pr", pr)
    datum.put("tasmin", tasmin)
    datum.put("tasmax", tasmax)

    return datum
  }

  def getModel(pr_path: String, tasmin_path: String, tasmax_path: String): String ={

    val pr_model = pr_path.split("/").last.split("_")(5)
    val tasmin_model = tasmin_path.split("/").last.split("_")(5)
    val tasmax_model = tasmax_path.split("/").last.split("_")(5)

    assert(pr_model == tasmin_model, "Model for file " + pr_model + " and " + tasmin_model + " were not the same!")
    assert(tasmin_model == tasmax_model,  "Model for file " + tasmin_model + " and " + tasmax_model + " were not the same!")

    return pr_model
  }

  def getShape( pr: Variable, tasmin: Variable, tasmax: Variable): Tuple3[Int, Int, Int] = {
    val time = min( pr.getDimension(0).getLength, tasmin.getDimension(0).getLength, tasmax.getDimension(0).getLength)
    val lat = min( pr.getDimension(1).getLength, tasmin.getDimension(1).getLength, tasmax.getDimension(1).getLength)
    val lon = min( pr.getDimension(2).getLength, tasmin.getDimension(2).getLength, tasmax.getDimension(2).getLength)
    return (time, lat, lon)
  }

  def getVariableValues(pr_nc_file: NetcdfFile, tasmin_nc_file: NetcdfFile, tasmax_nc_file: NetcdfFile): Tuple3[Array[Long], Array[Float], Array[Float]] ={


      // Actually we should really be doing some sanity checking here to
      // make sure tasmin/tasmax have the same dimensions/values
      val date_converter = new DateUnit(pr_nc_file.findVariable("time").getUnitsString())

      val raw_time = pr_nc_file.findVariable("time").read.copyTo1DJavaArray().asInstanceOf[Array[Double]]
      val time = raw_time.map(x => date_converter.makeDate(x).getTime() / 1000)

      var lat = pr_nc_file.findVariable("lat").read.copyTo1DJavaArray().asInstanceOf[Array[Float]]
      var lon = pr_nc_file.findVariable("lon").read.copyTo1DJavaArray().asInstanceOf[Array[Float]]

      return (time, lat, lon)
  }


  def main(args: Array[String]): Unit = {
    // should assert 3 values in args
    val pr_path :: tasmin_path :: tasmax_path :: rest = args.toList

    val model = getModel(pr_path, tasmin_path, tasmax_path)

    val pr_nc_file = readNetCDF(pr_path).get
    val tasmin_nc_file = readNetCDF(tasmin_path).get
    val tasmax_nc_file = readNetCDF(tasmax_path).get

    val (time, lat, lon) = getVariableValues(pr_nc_file, tasmin_nc_file, tasmax_nc_file)

    val pr = pr_nc_file.findVariable("pr")
    val tasmin = tasmin_nc_file.findVariable("tasmin")
    val tasmax = tasmax_nc_file.findVariable("tasmax")

    val shape = getShape(pr, tasmin, tasmax)



    val parser: Schema.Parser = new Schema.Parser()
    val schema = parser.parse(
      getClass.getResourceAsStream("/gddp.avsc")
    )

    val path = new Path("data.parquet")
    val writer = new AvroParquetWriter[GenericRecord](path, schema)

    var origin = Array(0, 0, 0)
    val size = Array(1, shape._2, shape._3)

    for(t_step <- 1 until shape._1 ){
      val pr_chunk = pr.read(origin, size).reduce.copyToNDJavaArray().asInstanceOf[Array[Array[Float]]]
      val tasmin_chunk = tasmin.read(origin, size).reduce.copyToNDJavaArray().asInstanceOf[Array[Array[Float]]]
      val tasmax_chunk = tasmax.read(origin, size).reduce.copyToNDJavaArray().asInstanceOf[Array[Array[Float]]]

      for (lat_step <- 1 until shape._2) {
        for (lon_step <- 1 until shape._3) {

          val d = getDatum(schema,
            lat(lat_step),
            lon(lon_step),
            time(t_step),
            model,
            pr_chunk(lat_step)(lon_step),
            tasmin_chunk(lat_step)(lon_step),
            tasmax_chunk(lat_step)(lon_step))
          writer.write(d)
        }
      }
      origin = Array(t_step, 0, 0)

    }


    writer.close()


    // val x = DenseVector.zeros[Double](5)


    // Bring this back once we're ready to do space filling curves
    // val t = toBreezeMatrix[Float](pr, "pr")


    /*
  val pr_fillValue = pr_nc_file.findVariable("pr").findAttribute("_FillValue").getNumericValue
  val tasmin_fillValue = pr_nc_file.findVariable("tasmin").findAttribute("_FillValue").getNumericValue
  val tasmax_fillValue = pr_nc_file.findVariable("tasmax").findAttribute("_FillValue").getNumericValue
*/


  }
}
