package com.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.cassandra
import org.apache.spark.sql.cassandra._

object Cassandra {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("Cassandra").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
  val edf = spark.read.format ("org.apache.spark.sql.cassandra").option("keyspace","vh_ks").option("table","emp").load()
  val adf = spark.read.format ("org.apache.spark.sql.cassandra").option("keyspace","vh_ks").option("table","asl").load()

  edf.createOrReplaceTempView("T1")
  adf.createOrReplaceTempView("T2")

  val res = spark.sql("select T1.*, T2.* from T1 join T2 on T1.first_name=T2.name").drop("first_name")

  res.write.mode(SaveMode.Append).format("org.apache.spark.sql.cassandra").option("keyspace","vh_ks").option("table","jointab").save()

  res.show()

    spark.stop()
  }
}
