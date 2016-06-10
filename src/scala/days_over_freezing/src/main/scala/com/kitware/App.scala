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
    import sqlContext.implicits._

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

    val of = df.withColumn("o_f", over_freezing($"tasmin"))

    val spec = Window.partitionBy("lat", "lon").orderBy("time").rowsBetween(-200, 0)

    val over = of.select($"*", sum($"o_f").over(spec) as "days_over")

    val wine_locs = over.select($"lat", $"lon").where($"days_over" >= 200).distinct()

    val wine = over.join(wine_locs, Seq("lat", "lon"))

    wine.write.format("parquet").save(args(1))

    // User defined function for average temperature in fahrenheit
    def _k_to_f(k: Double): Double = ((k - 273.15) * 1.8) + 32.0
    val avg_tmp_f = udf((min: Double, max: Double) => (_k_to_f(min) + _k_to_f(max)) / 2.0)

    // See: https://en.wikipedia.org/wiki/Winkler_scale
    // Months for northern/southern hemisphere
    val grow_season = parquet.select($"*", avg_tmp_f($"tasmin", $"tasmax") as "f_tasavg").filter(
      "(lat >= 0.0 AND month >= 4 AND month <= 10) OR (lat < 0.0 AND month <= 4 AND month >= 10)")


    // UDF for degree_days
    val degree_days = udf((temp: Double) => if( (temp.toInt - 50) <= 0 ) 0 else temp.toInt - 50)


    val anual_degree_days = grow_season.select($"*", degree_days($"f_tasavg") as "degree_days").groupBy($"year", $"lat", $"lon").agg("degree_days" -> "sum").withColumnRenamed("sum(degree_days)", "degree_days")


    def _winkler_scale(d: Double): Int = {
      if( d <= 2500.0 ) 1
      else if( d >= 2501.0 && d <= 3000.0) 2
      else if( d >= 3001.0 && d <= 3500.0) 3
      else if( d >= 3501.0 && d <= 4000.0) 4
      else 5
    }

    val winkler_scale = udf(_winkler_scale(_: Double))

    val winkler = anual_degree_days.withColumn("winkler", winkler_scale($"degree_days"))

    winkler.write.format("csv").save(args(2))

  }

}
