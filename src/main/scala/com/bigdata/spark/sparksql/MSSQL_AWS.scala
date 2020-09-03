package com.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import com.bigdata.spark.sparksql.allfunctions._

object MSSQL_AWS {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("MSSQL_AWS").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val url ="jdbc:sqlserver://mdabdenmssql.ck6vgv6qae1n.us-east-2.rds.amazonaws.com:1433;databaseName=rafidb;"
    val prop = new java.util.Properties()
    prop.setProperty("user","msuername")
    prop.setProperty("password","mspassword")
    prop.setProperty("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
    val df = spark.read.jdbc(url,"DEPT",prop)
    df.show()

  // Using AllFunctions:
  //val df = spark.read.jdbc(msurl,"DEPT",msprop)
  //df.show()

  //
  //val msdf = spark.read.format("jdbc")
  //  .option("url",msurl)
  //  .option("dbtable","DEPT")
  //  .option("user","msuername")
  //  .option("password","mspassword")
  //  .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
  //  .load()
  // msdf.show()

    spark.stop()
  }
}
