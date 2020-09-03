package com.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object JSON {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("JSON").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val data = "file:///C:\\bigdata\\datasets\\zips.json"
    val df = spark.read.format("json").load(data)
    df.printSchema()
    spark.stop()
  }
}
