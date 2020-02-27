package quickstart

import org.apache.spark.sql.SparkSession

object SparkSQLWordCount {
  def main(args: Array[String]): Unit = {
    //1.构建sparkSQL中和新对象SparkSession
    val spark = SparkSession.builder().appName("wordcount").master("local[*]").getOrCreate()

    //2. 通过sparkSession对象构建dataset或者dataframe
    val rdd = spark.sparkContext.makeRDD(List("Hello Hadoop", "Hello Kafka"))

    //scala隐式转换
    import spark.implicits._

    //rdd转换为ds或df
    val dataset = rdd.toDS()

    //强类型操作（操作的是类型）    无类型操作（操作的是字段）
  //方法1：
    /*dataset
      .flatMap(_.split("\\s"))
      .map((_,1))
      .groupBy("_1")//无类型操作  _1 第一列值word
      .sum("_2")//无类型操作 _2 1+1
      .show()*/

    //方法2：
   val flatMapDS = dataset.flatMap(_.split("\\s"))

    //基于flatMapDS创建一个表t_word
    flatMapDS.createTempView("t_word")

    spark
      .sql("select value,count(value) from t_word group by value")
      .show()


    spark.stop()



  }

}
