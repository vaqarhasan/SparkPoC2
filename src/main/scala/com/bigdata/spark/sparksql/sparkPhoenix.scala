package com.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object sparkPhoenix {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("sparkPhoenix").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    //val tab = args(0)
    import spark.implicits._
    import spark.sql
    //read data from phoenix
    //val df = spark.read.format("org.apache.phoenix.spark").option("table",tab).option("zkUrl","localhost:2181").load()
    //  df.show()
    // val data = "C:\\bigdata\\datasets\\us-500.csv"
    val data = args(0)
    val df1 = spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
    //before export data to phoenix first create a table in phoenix
    df1.write.mode(SaveMode.Overwrite).format("org.apache.phoenix.spark").option("table","ustab").option("zkUrl","localhost:2181").save()

    spark.stop()
  }
}
