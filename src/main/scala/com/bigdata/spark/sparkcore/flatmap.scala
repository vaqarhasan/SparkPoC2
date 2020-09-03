package com.bigdata.spark.sparkcore

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object flatmap {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("flatmap").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val nums = Array(1,2,3,4,444,5,6,7,6,5,4,3,2,11,21,32,43,54,65)
    // i want to separate its even and odd
//   val res = nums.groupBy(x=>x%2==0)

  //  println(res)

    val data = "C:\\bigdata\\datasets\\us-500.csv"
    val usrdd = sc.textFile(data)
val fst = usrdd.first()
    val reg = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"
    // skip nested comma if anywhere exists

    val res = usrdd.filter(x=>x!=fst).map(x=>x.split(reg)).map(x=>(x(6).replaceAll("\"",""),x(1).replaceAll("\"","")))
      .groupByKey()
    // .map(x=>(x,1)).groupByKey().map(x=>(x._1,x._2.sum))
     // .map(x=>(x,1)).reduceByKey((a,b)=>a+b)

    res.collect.foreach(println)

    spark.stop()
  }
}
//map : apply a logic/functionality on top of each and every element
//in map how many input elements u have same number output (number of elements) you will get

// filter apply a logic on top of each & every element based on bool value.


