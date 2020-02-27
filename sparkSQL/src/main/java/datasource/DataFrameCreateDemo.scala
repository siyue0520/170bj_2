package datasource

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object DataFrameCreateDemo {
  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder().appName("datasource").master("local[*]").getOrCreate()
    sparkSession.sparkContext.setLogLevel("ERROR")

    import sparkSession.implicits._

    //样例类构建rdd
    /*val rdd = sparkSession.sparkContext.makeRDD(List(Person(1,"zs"),Person(2,"ls")))*/

    //元组
    /*val rdd = sparkSession.sparkContext.makeRDD(List((1,"zs"),(2,"ls")))
    val df = rdd.toDF()*/
    //val df = rdd.toDF("newID","newName")

    //json数据
    /*val rdd = sparkSession.read.json("E:\\IdeaProjects2018\\sparkSQL\\src\\main\\resources")
    val df = rdd.toDF()*/

    //文件系统（重点）
    /*val rdd = sparkSession.sparkContext.textFile("hdfs://192.168.127.201:9000/checkpoint170_4/checkpoint-1581437579000.bk")
    val df = rdd.toDF()*/
    //RDD[Row]转换为DF（重点）
    //RDD[Person] ---> RDD[Row]
    val rdd = sparkSession.sparkContext.makeRDD(List(Person(1, "zs"), Person(2, "ls")))
    val rdd2:RDD[Row] = rdd.map(Person => Row(Person.id, Person.name))

    //structType 主要定义的是结果表的schema结构
    val structType = new StructType()
      .add("id", IntegerType, false, "注释1") // 列名 + 数据类型 + 是否允许为空
      .add("name", StringType, true)

      // RDD[Row]  ---> DF 参数2需要一个结构化类型structType
      sparkSession.createDataFrame(rdd2,structType)

      .show()


  }
}

case class Person(id:Int,name:String)
