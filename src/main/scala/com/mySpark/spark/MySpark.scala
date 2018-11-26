package com.mySpark.spark

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession


//https://zhuanlan.zhihu.com/p/50493420
//https://blog.csdn.net/high2011/article/details/79487796
object MySpark {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").appName("mySpark").getOrCreate()

    val sc = spark.sparkContext

    val jobs = spark.read.json("/Users/gongwenzhou/IdeaProjects/mySpark/src/main/scala/com/mySpark/spark/jobs")
    jobs.createTempView("session_job") //生命周期与SparkSession相关,仅限于当期会话
    jobs.createGlobalTempView("global_job")//生命周期与spark应用程序相关,跨会话,应用程序停止自动删除     SELECT * FROM global_temp.view1
//    spark.sql("select * from session_job").show(100,false)

//    LATERALVIEW(spark)




//    jobs.printSchema()

    sc.stop()
  }

  def LATERALVIEW(spark:SparkSession,sc:SparkContext): Unit ={

    val arrayTuple = (Array(1,2,3), Array("a","b","c"))

    import spark.implicits._
    val ncs = sc.makeRDD(Seq(arrayTuple)).toDF("ns", "cs")
    ncs.show()
    ncs.createOrReplaceTempView("ncs")
    ncs.sqlContext.sql("SELECT cs,n FROM ncs" +
      " LATERAL VIEW explode(ns) nsExpl AS n").show()

    ncs.sqlContext.sql("SELECT c,n FROM ncs" +
      " LATERAL VIEW explode(cs) csExpl AS c" +
      " LATERAL VIEW explode(ns) nsExpl AS n").show()



    spark.sql(
      "SELECT " +
        "positionCol.salary,COUNT(positionCol.salary) salary_COUNT from session_job " +
        "LATERAL VIEW " +
        "explode(content.positionResult.result) positionTable AS positionCol " +
        "WHERE  " +
        "content.positionResult.queryAnalysisInfo.positionName='java' " +
        "GROUP BY " +
        "positionCol.salary " +
        "ORDER BY " +
        "salary_count " +
        "DESC"
    ).show(false)
  }

}
