package com.bigdata.spark.sparkcore

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
// in this dataset who donated funds most frequently listed, find how donated maximum time.
object reducebykey2 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("reducebykey2").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val data = "C:\\bigdata\\datasets\\donations.txt"
    val brdd = sc.textFile(data)
    val res = brdd.map(x=>x.split(",")).map(x=>(x(0),x(1).toInt)).reduceByKey((a,b)=>a+b).sortBy(x=>x._2,false)
    res.take(7).foreach(println)
    spark.stop()
  }
}
