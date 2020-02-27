package lianxi

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window

/*
A表里面有三笔记录 字段是 ID start_date end_date
数据是：

1 2018-02-03 2019-02-03;
2 2019-02-04 2020-03-04;
3 2018-08-04 2019-03-04；

根据已知的三条记录用sql写出结果为：

A 2018-02-03 2018-08-04;
B 2018-08-04 2019-02-03;
C 2019-02-03 2019-02-04;
D 2019-02-04 2019-03-04;
E 2019-03-04 2020-03-04;
*/

object Test {
  def main(args: Array[String]): Unit = {

    //1. 构建Spark SQL中核心对象SparkSession
    val spark = SparkSession.builder().appName("wordcount").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    val df = spark.sparkContext.makeRDD(List(
      (1, "2018-02-03", "2019-02-03"),
      (2, "2019-02-04", "2020-03-04"),
      (3, "2018-08-04", "2019-03-04")
    )).flatMap(t3 => {
      Array[String](t3._2, t3._3)
    }).toDF("value")

    import org.apache.spark.sql.functions._
    val w1 = Window.rowsBetween(0,1)
    df
      .sort($"value")
      .withColumn("next",max("value")over(w1))
      .dropDuplicates("next")
      .show()


    spark.stop()
  }
}
