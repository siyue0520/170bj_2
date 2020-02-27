package operation.strong

import org.apache.spark.sql.SparkSession

/**
  * dataset的无类型操作
  */
object DatasetUnTypedOperationDemo {
  def main(args: Array[String]): Unit = {

    //1. 构建Spark SQL中核心对象SparkSession
    val sparkSession = SparkSession.builder().appName("wordcount").master("local[*]").getOrCreate()

    //2. 通过spark session对象构建dataset或者dataframe
    val rdd = sparkSession.sparkContext.makeRDD(List("Hello Hadoop", "Hello Kafka"))

    // rdd转换为ds或者df
    import sparkSession.implicits._

    val ds = rdd
                .flatMap(_.split("\\s"))
                .map((_,1))
                .toDS()

    ds
      .groupBy($"_1")// 隐式转换 $"列名"  string列 隐式转换为Column类型
      //.groupBy("_1")    // 隐式转换 $"列名"  string列 隐式转换为Column类型
      .sum("_2")
      .show()

    sparkSession.stop()

  }
}
