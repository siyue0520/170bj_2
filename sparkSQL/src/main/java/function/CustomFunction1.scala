package function

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}

/**
  * 自定义单行函数
  */
object CustomFunction1 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("custom function").master("local[*]").getOrCreate()
    //
    spark.udf.register("sex_convert",(sex:Boolean) => {
      sex match {
        case true => "男"
        case false => "女"
        case _ => null
      }
    })

    // sex_convert(sex) ---> '男' 或 '女'
    import spark.implicits._
    val rdd = spark
      .sparkContext
      .makeRDD(List(
        (1, "zs", true, 1, 15000),
        (2, "ls", false, 2, 18000),
        (3, "ww", false, 2, 14000),
        (4, "zl", false, 1, 18000),
        (5, "win7", false, 1, 18000)
      ))
    val df = rdd.toDF("id", "name", "sex", "dept", "salary")

    df.createOrReplaceTempView("t_user")

    spark
      .sql("select id,name,sex_convert(sex),dept,salary from t_user")
      .show()

    spark.stop()



  }

}
