package com.kitware.nex.monthly_means

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.hadoop.io._
import com.kitware.nex.DateTimeWritable
import org.apache.hadoop.conf.Configuration



import scala.collection.JavaConversions._
import java.io.FileWriter
import java.io.BufferedWriter
import scala.collection.mutable.ListBuffer
import au.com.bytecode.opencsv.CSVWriter

object Main {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Monthly Means: " + args(0))
    val sc = new SparkContext(conf)


    val out = new BufferedWriter(new FileWriter(args(1)));
    val writer = new CSVWriter(out);


    val hdfs_root = new Configuration().get("fs.defaultFS")

    val seq = sc.sequenceFile[DateTimeWritable, ArrayPrimitiveWritable](hdfs_root + "/user/nex/" + args(0) + "*.seq").map{ case (k, v) => (k.get(), v.get().asInstanceOf[Array[Float]])}

    val rdd = seq.map{ case (k, v) => ((k.getYear(), k.getMonthOfYear()) , v.sum / v.length) }

    val agg = rdd.aggregateByKey((0.0, 0))( (acc, v) => (acc._1 + v, acc._2 + 1),
      (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))

    val retval = agg.map{ case( k, v ) => (k, v._1 / v._2) }.collect()

    println(retval.deep.mkString("\n"))

    val flatten = retval.map{ case (k, v) => Array(k._1, k._2, v).map( x => x.toString) }

    writer.writeAll(ListBuffer(flatten.toList: _*))
    writer.close()
  }


}
