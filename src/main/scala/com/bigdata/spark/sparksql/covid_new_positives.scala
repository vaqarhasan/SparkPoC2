package com.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object covid_new_positives {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("covid_new_positives").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val data = "C:\\bigdata\\datasets\\covid\\*.xlsx"
    val df = spark.read.format("com.crealytics.spark.excel")
      .option("location", data)
      .option("useHeader", "true")
      .option("treatEmptyValuesAsNulls", "true")
      .option("inferSchema","true")
      .option("addColorColumns", "False")
      .load(data)

    df.show()
    spark.stop()
  }
}
