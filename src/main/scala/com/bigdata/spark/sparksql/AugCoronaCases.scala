package com.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object AugCoronaCases {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("AugCoronaCases").getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql

    val data = "C:\\bigdata\\datasets\\11augcoronacases.csv"
    val df = spark.read.format("csv").option("header","true").option("inferSchema","true").option("dateFormat","dd/MM/YYYY").load(data)
      .withColumn("SAMPLE SENT DATE", regexp_replace($"SAMPLE SENT DATE","\\.","-"))
      .withColumn("SAMPLE RESULT DATE", regexp_replace($"SAMPLE RESULT DATE","\\.","-"))

    val reg = "[^a-zA-Z]"
    val cols = df.columns.map(x=>x.replaceAll(reg,""))
    val ndf = df.toDF(cols:_*)

    ndf.createOrReplaceTempView("tab")
   val res = spark.sql("select SNO, NAME,AGE, replace(replace(SEX, 'FEMALE','F'), 'MALE','M') SEX, ADDRESS, SAMPLEID, SAMPLESENTDATE, SAMPLERESULTDATE, TEST from tab")
   res.show(truncate=false)

   val res1 = spark.sql("select max(ADDRESS) as Maximum_Cases_Area from tab")
   res1.show()

   val res2 = spark.sql("SELECT " +
     "sum (CASE WHEN AGE <= 1 THEN 01 ELSE 0 END) Less_Than_1_Yr," +
     "sum (CASE WHEN AGE >= 01 AND AGE <= 10 THEN 01 ELSE 0 END) 01_to_10_Yrs," +
     "sum (CASE WHEN AGE >= 11 AND AGE <= 20 THEN 01 ELSE 0 END) 11_to_20_Yrs, " +
     "sum (CASE WHEN AGE >= 21 AND AGE <= 30 THEN 01 ELSE 0 END) 21_to_30_Yrs, " +
     "sum (CASE WHEN AGE >= 31 AND AGE <= 40 THEN 01 ELSE 0 END) 31_to_40_Yrs, " +
     "sum (CASE WHEN AGE >= 41 AND AGE <= 50 THEN 01 ELSE 0 END) 41_to_50_Yrs, " +
     "sum (CASE WHEN AGE >= 51 AND AGE <= 60 THEN 01 ELSE 0 END) 51_to_60_Yrs, " +
     "sum (CASE WHEN AGE >= 61 AND AGE <= 70 THEN 01 ELSE 0 END) 61_to_70_Yrs, " +
     "sum (CASE WHEN AGE >= 71 AND AGE <= 80 THEN 01 ELSE 0 END) 71_to_80_Yrs, " +
     "sum (CASE WHEN AGE >= 81 AND AGE <= 90 THEN 01 ELSE 0 END) 81_to_90_Yrs, " +
     "sum (CASE WHEN AGE >= 90 THEN 01 ELSE 0 END) 90Plus " +
     "from tab")

   res2.show()

   spark.stop()
  }
}
