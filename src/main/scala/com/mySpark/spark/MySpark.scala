package com.mySpark.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object MySpark {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").appName("mySpark").getOrCreate()

    val sc = spark.sparkContext

    val list = List("1","2","3","4","5")
    val listRDD: RDD[String] = sc.makeRDD(list)

    listRDD.foreach(println)


    sc.stop()
  }

}
