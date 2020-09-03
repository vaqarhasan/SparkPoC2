package com.bigdata.spark.sparkcore

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object mapfilter {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("testing").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val data = "C:\\bigdata\\datasets\\bank-full.csv"
    val brdd = sc.textFile(data)
    val fst = brdd.first()
    val res = brdd.filter(x=>x!=fst).map(x=>x.replaceAll("\"",""))
      .map(x=>x.split(";"))
      .map(x=>(x(0).toInt,x(1),x(2),x(3),x(4),x(5).toInt,x(6),x(7),x(8),x(9),x(10)))
      .filter(x=>x._6>10000 && x._2=="retired")

    res.take(num=10).foreach(println)

    spark.stop()
  }
}
