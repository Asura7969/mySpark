package com.mySpark.spark.udaf

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

class NumsAvg extends UserDefinedAggregateFunction{

  //聚合函数的输入参数的类型
  override def inputSchema: StructType = StructType(StructField("inputColumn", DoubleType) :: Nil)

  //聚合buffer里的值得数据类型
  override def bufferSchema: StructType = StructType(StructField("count", LongType) :: StructField("avg", DoubleType) :: Nil)

  //返回值的数据类型
  override def dataType: DataType = DoubleType

  //对于相同的输入函数是否返回相同的输出
  override def deterministic: Boolean = true

  //初始化buffer
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0l
    buffer(1) = 0.0
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0)) {
      buffer(0) = buffer.getLong(0) + 1
      buffer(1) = buffer.getDouble(1) + input.getDouble(0)
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getDouble(1) + buffer2.getDouble(1)
  }

  override def evaluate(buffer: Row): Any = {
    val t = buffer.getDouble(1) / buffer.getLong(0)
    f"$t%1.5f".toDouble
  }
}
