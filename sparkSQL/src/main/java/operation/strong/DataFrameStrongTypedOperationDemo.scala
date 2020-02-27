package operation.strong

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.scalalang.typed

/**
  * dataset的强类型操作
  */
object DataFrameStrongTypedOperationDemo {
  def main(args: Array[String]): Unit = {

    //1. 构建Spark SQL中核心对象SparkSession
    val spark = SparkSession.builder().appName("wordcount").master("local[*]").getOrCreate()

    //2. 通过spark session对象构建dataset或者dataframe
    val rdd = spark.sparkContext.makeRDD(List("Hello Hadoop", "Hello Kafka"))
    // rdd转换为ds或者df
    import spark.implicits._

    //方法一：  df 实际 RDD[Row]
    /*val df = rdd.toDF()
    df
      .flatMap(row => row.getString(0).split("\\s"))
      .map((_,1))
      .groupByKey(t2 => t2._1)
      .agg(typed.sum(t2 => t2._2))
      .show()*/
  //方法二：
    val df = rdd.flatMap(_.split("\\s")).map((_,1)).toDF()

    df
      .groupByKey(Row => Row.getString(0))   // 0 代表的是行对象的第一值
      //.groupByKey(row => row.getAs[String]("_1")) //数据类型和列名
      .agg(typed.sum(row => row.getInt(1)))
      //.agg(typed.sum(row => row.getAs[Int]("_2")))
      .show()

    spark.stop()

  }
}
