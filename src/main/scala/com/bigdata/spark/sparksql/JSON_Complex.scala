package com.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object JSON_Complex {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("JSON_Complex").getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val data = "file:///C:\\bigdata\\datasets\\world_bank.json"
    val df = spark.read.format("json").load(data)
    df.createOrReplaceTempView("tab")
    val query = """select
      sector1.Name Sector1_Name, sector1.Percent Sector1_Percent,
      sector2.Name Sector2_Name, sector2.Percent Sector2_Percent,
      mp.Name mpname, mp.Percent mppercent,
      mn.Code mncode, mn.name mnname
      from tab
      lateral view explode(majorsector_percent) t as mp
      lateral view explode(mjsector_namecode) t as mn
      """
    val res = spark.sql(query)
    res.show(5)

    spark.stop()
  }
}
