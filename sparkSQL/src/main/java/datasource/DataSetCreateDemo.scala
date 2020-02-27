package datasource

import java.util.regex.{Matcher, Pattern}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.scalalang.typed

object DataSetCreateDemo {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder().appName("datasource").master("local[*]").getOrCreate()
    sparkSession.sparkContext.setLogLevel("ERROR")
    //样例类构建rdd
    /*val rdd = sparkSession.sparkContext.makeRDD(List(Dog(1,"小黑"),Dog(2,"小白")))*/
    //Tuple元组构建rdd
    /*val rdd = sparkSession.sparkContext.makeRDD(List((1,"小黑",true),(2,"小白",false)))*/

    import sparkSession.implicits._

    /*val ds = rdd.toDS()
    ds.show()
    */
    //读取JSON数据文件
    /*val ds = sparkSession
                .read
                .json("E:\\IdeaProjects2018\\sparkSQL\\src\\main\\resources")
                .as[User]

    ds.show()*/

    //读取文件系统
    val rdd = sparkSession.sparkContext.textFile("hdfs://192.168.127.201:9000/checkpoint170_4/checkpoint-1581437579000.bk")
    val ds = rdd.toDS

    ds
      .map(line => {
          val regex: String = "^(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}).*\\[(.*)\\]\\s\"(\\w{3,6})\\s(.*)\\sHTTP\\/1.1\"\\s(\\d{3})\\s(.*)$"
          val pattern: Pattern = Pattern.compile(regex, Pattern.MULTILINE)
          val matcher: Matcher = pattern.matcher(line)
          matcher.find()
          val ip = matcher.group(1)
          val time = matcher.group(2)
          val method = matcher.group(3)
          val uri = matcher.group(4)
          val status = matcher.group(5)
          val responseSize = matcher.group(6)
          (ip, time, method, uri, status, responseSize)
        })
          // 统计状态码分布情况  200 2
          // Sql: select status,count(status) from t_log group by status
      .groupByKey(t6 => t6._5)
      .agg(typed.count(t6 => t6._5))
      .withColumnRenamed("value","status")
      .withColumnRenamed("TypedCount(scala.Tuple6)","count")
      .show()






    sparkSession.stop()

  }

}

case class Dog(id:Int,name:String)
case class User(id:Long,name:String,sex:Boolean)
