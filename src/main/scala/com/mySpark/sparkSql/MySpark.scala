package com.mySpark.sparkSql

import com.mySpark.sparkSql.udaf.NumsAvg
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable


//https://zhuanlan.zhihu.com/p/50493420
//https://blog.csdn.net/high2011/article/details/79487796
object MySpark {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setAppName("spark sql")
      .setMaster("local[*]")
      .set("spark.default.parallelism","4")
      .set("spark.sql.shuffle.partitions","4")

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val sc = spark.sparkContext

//    sql(spark,sc)
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

  def sql(spark:SparkSession,sc:SparkContext): Unit ={

    import org.apache.spark.sql.functions.{col,expr,sum}

    val sales = spark.createDataFrame(
      Seq( ("Warsaw", 2016, 100), ("Warsaw", 2017, 200), ("Warsaw", 2015, 100), ("Warsaw", 2017, 200), ("Beijing", 2017, 200), ("Beijing", 2016, 200),
        ("Beijing", 2015, 200), ("Beijing", 2014, 200), ("Warsaw", 2014, 200),
        ("Boston", 2017, 50), ("Boston", 2016, 50), ("Boston", 2015, 50),
        ("Boston", 2014, 150)))
      .toDF("city", "year", "amount")
    sales.show()

//    println("select ============================>")
//    sales.select("city","year","amount").show()
//    sales.select(col("city"),col("year"),col("amount")+1).show()
//    sales.selectExpr("city","year as date","amount+1").show()
//    sales.select(expr("city"),expr("year as date"),expr("amount+1")).show()
//    println("select ============================<")

//    println("filter ============================>")
//    sales.filter("amount>150").show()
//    sales.filter(col("amount") > 150).show()
//    sales.filter(row => row.getInt(2) > 150).show()
//    println("filter ============================<")

//    println("where ============================>")
//    sales.where("amount > 150").show()
//    sales.where(col("amount") > 150).show()
//    println("where ============================<")

//    println("group by ============================>")
//    sales.groupBy("city").count().show(10)
//    sales.groupBy(col("city")).agg(sum("amount").as("total")).show(10)
//    println("group by ============================<")

//    println("union ============================>")
//    //两个dataset的union操作这个等价于union all操作，所以要实现传统数据库的union操作，需要在其后使用distinct进行去重操作。
//    sales.union(sales).groupBy("city").count().show()
//    println("union ============================<")


//    println("join ============================>")
//
//    //相同的列进行join
//    sales.join(sales,"city").show(100)
//    //多列join
//    sales.join(sales,Seq("city","year")).show()
//
//    /**
//      * 指定join类型， join 类型可以选择:
//          `inner`, `cross`, `outer`, `full`, `full_outer`,
//          `left`, `left_outer`, `right`, `right_outer`,
//          `left_semi`, `left_anti`.
//      */
//    // 内部join
//    sales.join(sales,Seq("city","year"),"inner").show()
//
//    /**
//      * join条件 ：
//            可以在join方法里放入join条件，
//            也可以使用where,这两种情况都要求字段名称不一样。
//      */
//    sales.join(sales, col("city").alias("city1") === col("city")).show()
//    sales.join(sales).where(col("city").alias("city1") === col("city")).show()
//
//    /**
//      * dataset的self join 此处使用where作为条件，
//        需要增加配置.set("spark.sql.crossJoin.enabled","true")
//        也可以加第三个参数，join类型，可以选择如下：
//        `inner`, `cross`, `outer`, `full`, `full_outer`, `left`,
//        `left_outer`, `right`, `right_outer`, `left_semi`, `left_anti`
//      */
//    sales.join(sales,sales("city") === sales("city")).show()
//    sales.join(sales).where(sales("city") === sales("city")).show()
//
//    /**
//      * joinwith,可以指定第三个参数,join类型,类型可以选择如下：
//          `inner`, `cross`, `outer`, `full`, `full_outer`, `left`, `left_outer`,
//          `right`, `right_outer`
//      */
//    sales.joinWith(sales,sales("city") === sales("city"),"inner").show()
//
//    println("join ============================<")

//    println("order by ============================>")
//    sales.orderBy(col("year").desc,col("amount").asc).show()
//    sales.orderBy("city","year").show()
//    println("order by ============================<")

//    println("sort ============================>")
//    sales.sort(col("year").desc,col("amount").asc).show()
//    sales.sort("city","year").show()
//    println("sort ============================<")


//    println("sortwithinpartition ============================>")
//    //在分区内部进行排序，局部排序
//    sales.sortWithinPartitions(col("year").desc,col("amount").asc).show()
//    sales.sortWithinPartitions("city","year").show()
//    println("sortwithinpartition ============================<")

//    println("withColumn ============================>")
//
//    /**
//      * 假如列，存在就替换，不存在新增 withColumnRenamed 对已有的列进行重命名
//      */
//    //相当于给原来amount列，+1
//    sales.withColumn("amount",col("amount")+1).show()
//    // 对amount列+1，然后将值增加到一个新列 amount1
//    sales.withColumn("amount1",col("amount")+1).show()
//    // 将amount列名，修改为amount1
//    sales.withColumnRenamed("amount","amount1").show()
//    println("withColumn ============================<")


//    /**
//      * distinct
//      */
//    sales.distinct().show(10)
//
//
//    /**
//      * dropDuplicates
//      * 适用于dataset有唯一的主键，然后对主键进行去重
//      */
//    val before = sales.count()
//    val after = sales.dropDuplicates("city").count()
//    println("before ====> " +before)
//    println("after ====> "+after)

    /**
      * explain
      */
    sales.orderBy(col("year").desc,col("amount").asc).explain()
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
