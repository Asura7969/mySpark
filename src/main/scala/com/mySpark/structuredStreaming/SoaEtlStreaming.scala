package com.mySpark.structuredStreaming

import java.util
import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.to_json
import org.apache.spark.sql.streaming.Trigger
import utils.LogScanner

/**
  * spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.1 --class * --master yarn --deploy-mode client --driver-memory 2g --executor-memory 1g --executor-cores 3 --num-executors 5 ./AppDFMonitorPlatform.jar
  * */
object SoaEtlStreaming {
  def main(args: Array[String]): Unit = {

//    val config = getConfig("etl.properties")
    val config = new Properties()
    val conf = new SparkConf().setAppName(SoaEtlStreaming.getClass.getSimpleName)
      .setMaster("local[*]")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
//      .set("spark.streaming.backpressure.initialRate", "4000")
//      .set("spark.streaming.kafka.maxRatePerPartition", "7000")


    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    spark.sparkContext.setLogLevel("warn")

    import spark.implicits._
    val stream: Dataset[String] = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", config.getProperty(""))
      .option("subscribe", config.getProperty(""))
      .option("group.id",config.getProperty(""))
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[String]

    val keys = new util.ArrayList[String]()

    val broadcastKeys = spark.sparkContext.broadcast(keys)

    val transform = stream.map(t => LogScanner.scan(t, broadcastKeys.value))


    val frame = stream.select(to_json($"").as("json"))


//    val query = transform.writeStream
//      .format("kafka")
//      .option("kafka.bootstrap.servers", config.getProperty(SINK_METADATA_BROKER_LIST))
//      .option("topic", "soa-etl")
//      .trigger(Trigger.ProcessingTime(1, TimeUnit.SECONDS))
//      .start()


    val query = transform.writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", "false")
      .trigger(Trigger.ProcessingTime(1, TimeUnit.SECONDS))
      .start()

    query.awaitTermination()
  }
}
