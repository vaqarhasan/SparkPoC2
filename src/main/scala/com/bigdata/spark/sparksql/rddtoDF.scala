package com.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object rddtoDF {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("rddtoDF").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql

    val data = "C:\\bigdata\\datasets\\bank-full.csv"
    val brdd = sc.textFile(data)
    val fst = brdd.first()
    val clean = brdd.filter(x=>x!=fst).map(x=>x.replaceAll("\"",""))
      .map(x=>x.split(";"))
      .map(x=>(x(0).toInt,x(1),x(2),x(3),x(4),x(5).toInt,x(6),x(7),x(8),x(9),x(10),x(11),x(12),x(13),x(14),x(15),x(16)))

    val df =clean.toDF("age","job","marital","education","default","balance","housing","loan","contact","day","month","duration","campaign","pdays","previous","poutcome","y")
    df.show()
    df.createOrReplaceTempView("tab")
    val res = spark.sql("select * from tab where balance > 95000")
    res.show()

    spark.stop()
  }
}
