package com.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import com.bigdata.spark.sparksql.allfunctions._

object Oracle_AWS {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("Oracle_AWS").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val ourl ="jdbc:oracle:thin:@//sqooppoc.cjxashekxznm.ap-south-1.rds.amazonaws.com:1521/ORCL"
    val oprop = new java.util.Properties()
    oprop.setProperty("user","ousername")
    oprop.setProperty("password","opassword")
    oprop.setProperty("driver","oracle.jdbc.OracleDriver")
    val odf = spark.read.jdbc(ourl,"EMP",oprop)
    odf.show()

  // Using AllFunctions:
  //val odf = spark.read.jdbc(ourl,"EMP",oprop)
  //odf.show()

  //  val odf = spark.read.format("jdbc")
  //    .option("url",ourl)
  //    .option("dbtable","EMP")
  //    .option("user","ousername")
  //    .option("password","opassword")
  //    .option("driver","oracle.jdbc.OracleDriver")
  //    .load()
  //  odf.show()

    spark.stop()
  }
}
