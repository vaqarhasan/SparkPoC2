package com.bigdata.spark.sparksql
import com.bigdata.spark.sparksql.allfunctions._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object sparkimportall {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("sparkimportall").enableHiveSupport().getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
 //val tabs=Array("EMP","DEPT")
    val qry = "(select TABLE_NAME from all_tables where tablespace_name='USERS') t"
    val tabs = spark.read.jdbc(ourl,qry,oprop).rdd.map(x=>x(0)).collect.toList.filter(x=>x!="MYFIRSTTEST" && x!="EMP")

    tabs.foreach{ x=>
      println(s"importing $x table")
      val df = spark.read.jdbc(ourl,s"$x",oprop)
      df.show()
      df.write.mode(SaveMode.Append).format("hive").saveAsTable(s"$x")
    }
    spark.stop()
  }
}
