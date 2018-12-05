package com.mySpark.spark

import com.mySpark.spark.udaf.NumsAvg
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable


//https://zhuanlan.zhihu.com/p/50493420
//https://blog.csdn.net/high2011/article/details/79487796
object MySpark {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").appName("mySpark").getOrCreate()

    val sc = spark.sparkContext


//    test(spark,sc)
//    LATERALVIEW(spark,sc)
//    udfFunBySQL(spark,sc)
//    udfFunByDataFrame(spark,sc)
//    udaf(spark,sc)

    sc.stop()
  }

  def test(spark:SparkSession,sc:SparkContext): Unit ={
    val list = List(
      "{\"method\":\"create\",\"isBluetoothOpen\":\"true\",\"service\":\"com.hellobike.create\",\"logvalue\":\"5\",\"addCol\":\"1\"}",
      "{\"method\":\"create\",\"isBluetoothOpen\":\"false\",\"service\":\"com.hellobike.create\",\"logvalue\":\"4\",\"appid\":\"appid1\",\"addCol\":\"2\"}",
      "{\"method\":\"create\",\"code\":\"2\",\"service\":\"com.hellobike.create\",\"logvalue\":\"3\",\"appid\":\"appid2\",\"addCol\":\"2\"}",
      "{\"method\":\"create\",\"service\":\"com.hellobike.aa\",\"logvalue\":\"3\",\"appid\":\"appid2\",\"addCol\":\"null\"}")
    val rdd: RDD[String] = sc.parallelize(list)



    val df = spark.read.json(rdd)

    df.where("isBluetoothOpen = 'true'").show()
  }

  /**
    * 自定义 UDAF 函数
    */
  def udaf(spark:SparkSession,sc:SparkContext): Unit ={


    val nums = Array(4.5, 2.4, 1.8)
    val numsRDD = sc.parallelize(nums, 1)
    val numsRowRDD = numsRDD.map { x => Row(x) }
    val structType = StructType(Array(StructField("num", DoubleType, true)))

    val userDF: DataFrame = spark.createDataFrame(numsRowRDD,structType)

    userDF.createOrReplaceTempView("numtest")
    spark.sql("select avg(num) from numtest ").collect().foreach { x => println(x) }

    spark.udf.register("numsAvg", new NumsAvg)
    spark.sql("select numsAvg(num) from numtest ").collect().foreach { x => println(x) }

  }

  /**
    * 自定义函数(Spark Sql udf)
    * @param spark
    * @param sc
    */
  def udfFunBySQL(spark:SparkSession,sc:SparkContext): Unit ={
    val userData = Array(("Leo", 16,List("basketball","football")), ("Marry", 21,List("Dota2","LOL")), ("Jack", 14,List("run")), ("Tom", 18,null))
    val userDF = spark.createDataFrame(userData).toDF("name", "age","hobby")
    userDF.createOrReplaceTempView("user")

    spark.sql("SELECT * FROM user").show(false)

    //注册自定义函数（通过匿名函数）
    spark.udf.register("strLen", (str: String) => str.length())

    //注册自定义函数（通过实名函数）
    spark.udf.register("isAdult", isAdult _)
    spark.sql("select *,strLen(name) as name_len,isAdult(age) as isAdult from user").show(false)
  }

  /**
    * 自定义函数(Spark Sql udf)
    * @param spark
    * @param sc
    */
  def udfFunByDataFrame(spark:SparkSession,sc:SparkContext): Unit ={
    val userData = Array(("Leo", 16,List("basketball","football")), ("Marry", 21,List("Dota2","LOL")), ("Jack", 14,List("run")), ("Tom", 18,null))
    val userDF = spark.createDataFrame(userData).toDF("name", "age","hobby")
    userDF.createOrReplaceTempView("user")
    import org.apache.spark.sql.functions._
    spark.sql("SELECT * FROM user").show(false)

    //注册自定义函数（通过匿名函数）
    val strLen = udf((str: String) => str.length())

    //注册自定义函数（通过实名函数）
    val udf_isAdult = udf(isAdult _)
    //通过withColumn添加列
    userDF.withColumn("name_len", strLen(col("name"))).withColumn("isAdult", udf_isAdult(col("age"))).show(false)

    //通过select添加列
    userDF.select(col("*"), strLen(col("name")) as "name_len", udf_isAdult(col("age")) as "isAdult").show(false)
  }

  /**
    * 根据年龄大小返回是否成年 成年：true,未成年：false
    */
  def isAdult(age: Int): Boolean = if (age < 18) false else true


  def LATERALVIEW(spark:SparkSession,sc:SparkContext): Unit ={

    val jobs = spark.read.json("/Users/gongwenzhou/IdeaProjects/mySpark/src/main/scala/com/mySpark/spark/jobs")
    jobs.createTempView("session_job") //生命周期与SparkSession相关,仅限于当期会话
    jobs.createGlobalTempView("global_job")//生命周期与spark应用程序相关,跨会话,应用程序停止自动删除     SELECT * FROM global_temp.view1
//    spark.sql("select * from session_job").show(100,false)

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
