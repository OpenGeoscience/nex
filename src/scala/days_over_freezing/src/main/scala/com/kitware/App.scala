package com.kitware
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * @author ${user.name}
 */
object App {

  def foo(x : Array[String]) = x.foldLeft("")((a,b) => a + b)

  def main(args : Array[String]) {
    val conf = new org.apache.spark.SparkConf().setAppName("Days over Freezing")
    val sc = new org.apache.spark.SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val parquet = sqlContext.read.parquet(args(0))
    parquet.registerTempTable("parquet")

    val sql = """
     SELECT lat, lon, time, model, pr, tasmin, tasmax, MONTH(from_unixtime(time)) as month, YEAR(from_unixtime(time)) as year
     FROM parquet
     WHERE pr < 1.0E20 AND tasmin < 1.0E20 AND tasmax < 1.0E20
     """

    val df = sqlContext.sql(sql)

    // Over Freezing
    def _over_freezing(k: Double) = if (k > 273.15) 1 else 0
    val over_freezing = udf(_over_freezing(_: Double))

    val of = df.withColumn("o_f", over_freezing(col("tasmin")))

    val spec = Window.partitionBy("lat", "lon").orderBy("time").rowsBetween(-200, 0)

    val over = of.select(col("*"), sum(col("o_f")).over(spec) as "days_over")

    over.select("*").where(col("days_over") >= 200).write.format("csv").save(args(1))

  }

}
