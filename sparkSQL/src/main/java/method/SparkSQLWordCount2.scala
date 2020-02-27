package method

import org.apache.spark.sql.SparkSession

object SparkSQLWordCount2 {
  def main(args: Array[String]): Unit = {

    //1. 构建Spark SQL中核心对象SparkSession
    val spark = SparkSession.builder().appName("wordcount").master("local[*]").getOrCreate()

    m2(spark)


    /**
      * cube 多维度分组  数据立方体
      *
      *  最大好处：在进行数据分析时，根据cube字段的分组查询的结果一定在结果表
      *
      * @param spark
      */
    def m2(spark: SparkSession) = {
      // 来源数据源HDFS、HBase
      import spark.implicits._
      val df = spark
        .sparkContext
        .makeRDD(List(
          (110, 50, 80, 80),
          (120, 60, 95, 75),
          (120, 50, 96, 70)))
        .toDF("height", "weight", "IQ", "EQ")

      df
        // 列名 表示根据那几个字段的不同组合进行分组操作
        // group by 110,50
        // group by 120,60
        // group by 120,50
        // group by 110,60
        // group by 110,null
        // group by 120,null
        // ...
        .cube("height", "weight")
        .count()
        .show()

      spark.stop()

    }


    /**
      * 对两个结果表的Join连接操作
      * @param spark
      */
    def m1(spark: SparkSession): Unit ={
      val rdd1 = spark.sparkContext.makeRDD(List((1, "zs", "A"), (2, "ls", "B"), (3, "ww", "C")))
      val rdd2 = spark.sparkContext.makeRDD(List(("A", "市场部"), ("B", "后勤部")))

      import spark.implicits._
      val df1 = rdd1.toDF("id", "name", "dept_id")
      val df2 = rdd2.toDF("d_id", "dept_name")

      df1
        //.join(df2,$"dept_id"===$"d_id","left_outer")
        // 默认的连接类型为inner
        .join(df2, $"dept_id" === $"d_id")
        .show()
      spark.stop()
    }

  }
}
