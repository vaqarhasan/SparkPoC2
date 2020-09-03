package com.bigdata.spark.sparkcore

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object joindata {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("joindata").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
val aslrdd = sc.textFile("C:\\bigdata\\datasets\\asl.csv")
    val neprdd = sc.textFile("C:\\bigdata\\datasets\\nep.csv")
val skasl = aslrdd.first()
    val sknep = neprdd.first()

    val nres = neprdd.filter(x=>x!=sknep).map(x=>x.split(",")).map(x=>(x(0),x(1),x(2).toInt))
    val ares = aslrdd.filter(x=>x!=skasl).map(x=>x.split(",")).map(x=>(x(0),x(1).toInt,x(2)))
// to join rdd what column u want separate that
    val akb = ares.keyBy(x=>x._1)
    val nkb = nres.keyBy(x=>x._1)
    val join = akb.join(nkb).map(x=>(x._2._2._1,x._2._2._2,x._2._2._3,x._2._1._2,x._2._1._3))
    join.take(3).foreach(println)
    //nres.take(5).foreach(println)
    //ares.take(5).foreach(println)
    spark.stop()
  }
}
