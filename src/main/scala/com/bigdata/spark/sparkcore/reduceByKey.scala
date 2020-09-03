package com.bigdata.spark.sparkcore

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object reduceByKey {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[2]").appName("reduceByKey").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val data = "C:\\bigdata\\datasets\\bank-full.csv"
    val brdd = sc.textFile(data)
    val sumf = (x:Int, y:Int)=> x+y
    val fst = brdd.first()
    val res = brdd.filter(x=>x!=fst).map(x=>x.replaceAll("\"","")).map(x=>x.split(";"))
      .map(x=>(x(1),1)).reduceByKey(sumf).sortBy(x=>x._2,false)


    //.map(x=>(x(1),1)).reduceByKey((a,b)=>a+b).sortBy(x=>x._2,false)
    // reduce by key used to group the values basesed on key
    //b
    res.take(9).foreach(println)


    spark.stop()
  }
}
//Array("age";"job";"marital";"education";"default";"balance";"housing";"loan";"contact";"day";"month";"duration";"campaign";"pdays";"previous";"poutcome";"y")
//Array(58;"management";"married";"tertiary";"no";2143;"yes";"no";"unknown";5;"may";261;1;-1;0;"unknown";"no")

