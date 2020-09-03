package com.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.joda.time.DateTime

object covid_new_positives2 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("covid_new_positives2").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    import org.apache.spark.sql.types._

    val data = "file:///C:\\bigdata\\datasets\\covid\\"
    val simpleSchema = StructType(List(
      StructField("S.No", IntegerType,true),
      StructField("NAME", StringType, true),
      StructField("AGE", IntegerType,true),
      StructField("SEX", StringType, true),
      StructField("ADDRESS", StringType, true),
      StructField("SAMPLE ID", IntegerType, true),
      StructField("SAMPLE SENT DATE", DateType, true),
      StructField("SAMPLE RESULT DATE", DateType, true),
      StructField("TEST", StringType, true)
    ))
    val df = spark.read.schema(simpleSchema).format("csv").option("header","true").load(data)

/*      .withColumn("SAMPLE SENT DATE", regexp_replace($"SAMPLE SENT DATE","\\.","/"))
      .withColumn("SAMPLE SENT DATE", regexp_replace($"SAMPLE SENT DATE","-","/"))
      .withColumn("SAMPLE RESULT DATE", regexp_replace($"SAMPLE RESULT DATE","\\.","/"))
      .withColumn("SAMPLE RESULT DATE", regexp_replace($"SAMPLE RESULT DATE","-","/"))*/


    val reg = "[^a-zA-Z]"
    val cols = df.columns.map(x=>x.replaceAll(reg,""))
    val ndf = df.toDF(cols:_*)

    ndf.show()
    ndf.printSchema()

    spark.stop()
  }
}
