package com.kitware.nex

import ucar.nc2.NetcdfFile


object Hi {

  def main(args: Array[String]) = {
    printvars(NetcdfFile.open("/data/tmp/pr_day_BCSD_historical_r1i1p1_CSIRO-Mk3-6-0_1997.nc"))
  }

  private def printvars(ncfile: ucar.nc2.NetcdfFile): Unit = {
    println(ncfile.getVariables())
  }
}
