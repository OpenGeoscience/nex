package com.kitware.nex

import ucar.ma2.{ArrayDouble, ArrayFloat}
import ucar.nc2.{NetcdfFile, Variable}
import ucar.nc2.time.CalendarDate
import ucar.nc2.units.DateUnit

import breeze.linalg._

import scala.util.Try

/**
  * Created by kotfic on 2/4/16.
  */
object Gddp2Parquet {

  def readNetCDF(path: String) : Try[NetcdfFile] = Try(NetcdfFile.open(path))

  // See: https://github.com/rustandruin/test-scala-netcdf/blob/master/src/main/scala/netcdfTestCode.scala#L111-L123
  def toBreezeMatrix[T](nc: NetcdfFile, varName: String) : Tuple2[DenseMatrix[T], DenseMatrix[Boolean]] = {
    var variable = nc.findVariable(varName)
    var time = variable.getDimension(0).getLength
    var lat = variable.getDimension(1).getLength
    var lon = variable.getDimension(2).getLength
    var fillValue = variable.findAttribute("_FillValue").getNumericValue.asInstanceOf[T]
    var breezeMat = (new DenseVector(variable.read.copyTo1DJavaArray.asInstanceOf[Array[T]])).toDenseMatrix.reshape(time, lat*lon)
    var maskMat = breezeMat.copy.mapValues(_ == fillValue)
    return (breezeMat, maskMat)
  }

  def main(args: Array[String]): Unit ={
    val x = DenseVector.zeros[Double](5)
    val pr = readNetCDF(args(0)).get
    val t = toBreezeMatrix[Float](pr, "pr")
    print("Hello")
  }
}
