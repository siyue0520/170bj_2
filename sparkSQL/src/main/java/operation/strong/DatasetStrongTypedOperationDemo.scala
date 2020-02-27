package operation.strong

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.scalalang.typed

/**
  * dataset的强类型操作
  */
object DatasetStrongTypedOperationDemo {
  def main(args: Array[String]): Unit = {

    //1. 构建Spark SQL中核心对象SparkSession
    val spark = SparkSession.builder().appName("wordcount").master("local[*]").getOrCreate()

    //2. 通过spark session对象构建dataset或者dataframe
    val rdd = spark.sparkContext.makeRDD(List("Hello Hadoop", "Hello Kafka"))

    // rdd转换为ds或者df
    import spark.implicits._
    // scala隐式转换
    val dataset = rdd.toDS()

    // 方法一：
    dataset
      .flatMap(_.split("\\s"))
      .map((_, 1))
      .groupByKey(_._1)
      .agg(typed.sum(t2 => t2._2))
      .show() // 展示最终的处理结果

    spark.stop()



  }
}
