package operation.strong

import org.apache.spark.sql.SparkSession

object DataFrameUnTypedOperationDemo {
  def main(args: Array[String]): Unit = {

    //1. 构建Spark SQL中核心对象SparkSession
    val spark = SparkSession.builder().appName("wordcount").master("local[*]").getOrCreate()

    //2. 通过spark session对象构建dataset或者dataframe
    val rdd = spark.sparkContext.makeRDD(List("Hello Hadoop", "Hello Kafka"))

    // rdd转换为ds或者df
    import spark.implicits._

    val df = rdd
      .flatMap(line => line.split("\\s"))
      .map((_, 1))
      .toDF

    df
      .groupBy($"_1")
      .sum("_2")

      .filter("_1 != 'Hadoop'")
      .orderBy($"sum(_2)" desc)
      .limit(1)

      .show()

    spark.stop()



  }
}
