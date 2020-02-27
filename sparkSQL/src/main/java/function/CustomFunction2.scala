package function

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, StructType}

/**
  * 自定义聚合函数（my_sum） 类似于内置Sum
  */
object CustomFunction2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("custom function").master("local[*]").getOrCreate()
    // 参数一：函数名  参数二：用以进行聚合操作的函数对象
    spark.udf.register("my_sum",new UserDefinedAggregateFunction {
      override def inputSchema: StructType = ???

      override def bufferSchema: StructType = ???

      override def dataType: DataType = ???

      override def deterministic: Boolean = ???

      override def initialize(buffer: MutableAggregationBuffer): Unit = ???

      override def update(buffer: MutableAggregationBuffer, input: Row): Unit = ???

      override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = ???

      override def evaluate(buffer: Row): Any = ???
    })




  }
}
