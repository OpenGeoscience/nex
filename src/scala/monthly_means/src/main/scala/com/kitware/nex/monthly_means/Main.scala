package com.kitware.nex.monthly_means

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.hadoop.io._
import com.kitware.nex.DateTimeWritable
import org.apache.hadoop.conf.Configuration

object Main {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Monthly Means")
    val sc = new SparkContext(conf)

    val hdfs_root = new Configuration().get("fs.defaultFS")

    val seq = sc.sequenceFile[DateTimeWritable, ArrayPrimitiveWritable](hdfs_root + "/user/nex/" + args(0) + "*.seq").map{ case (k, v) => (k.get(), v.get().asInstanceOf[Array[Float]])}

    val rdd = seq.map{ case (k, v) => (k.getMonthOfYear(), v.sum / v.length) }

    val agg = rdd.aggregateByKey((0.0, 0))( (acc, v) => (acc._1 + v, acc._2 + 1),
      (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))

    println(agg.map{ case( k, v ) => (k, v._1 / v._2) }.collect())

  }


}
