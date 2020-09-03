package com.bigdata.spark.streaming

import org.apache.spark.sql._
import org.apache.spark.streaming._
import org.apache.spark.sql.functions._
import com.bigdata.spark.sparksql.allfunctions._

object sparkStreamingWH {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("sparkStreamingWH").getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
//    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val lines = ssc.socketTextStream("15.207.86.58", 1234)

    //lines.print()
    lines.foreachRDD { x =>
      val spark = SparkSession.builder.config(x.sparkContext.getConf).getOrCreate()
      import spark.implicits._
      val df = x.map(x=>x.split(",")).map(x=>(x(0),x(1),x(2))).toDF("name","age","city")
      df.show()
      df.createOrReplaceTempView("tab")
      val res = spark.sql("select * from tab where city='mas'")
      val res1 = spark.sql("select * from tab where city='del'")
      res.write.mode(SaveMode.Append).jdbc(ourl, table="masinfo", oprop)
      res1.write.mode(SaveMode.Append).jdbc(ourl, table="delhiinfo", oprop)
    }
    ssc.start()             // Start the computation
    ssc.awaitTermination()
  }
}
