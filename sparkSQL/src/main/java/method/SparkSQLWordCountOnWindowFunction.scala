package method

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window

object SparkSQLWordCountOnWindowFunction {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder().master("local[*]").appName("wordcount").getOrCreate()
    sparkSession.sparkContext.setLogLevel("ERROR")
    import sparkSession.implicits._

    val df = sparkSession.sparkContext.makeRDD(List(
      (1, "zs", true, 1, 15000),
      (2, "ls", false, 2, 18000),
      (3, "ww", false, 2, 14000),
      (4, "zl", false, 1, 18000),
      (5, "win7", false, 1, 16000)
    )).toDF("id","name","sex","dept","salary")


    import org.apache.spark.sql.functions._

    val w1 = Window
                .partitionBy("dept")

    val w2 = Window
                .partitionBy("dept")
                .orderBy("salary")

    val w3 = Window
                .partitionBy("dept")
                .orderBy($"salary" desc)
                //窗口内的有效数据范围
                //value = 0  当前行
                // n 当前行的下n行
                //-n 当前行的上n行
                .rowsBetween(-1,1)
    df
      /*.selectExpr("dept","salary")
      .groupBy("dept","salary")
      .count()*/
      //获取各个部门最高工资
      .withColumn("max_dept_salary",max("salary")over(w1))
      //获取各个部门平均工资
      .withColumn("avg_dept_salary",avg("salary")over(w1))
      // 紧密排名 1 2 2 3
      .withColumn("rank",dense_rank()over(w2))
      //数据分析窗口函数 lag 取前n行的数据
      .withColumn("lag",lag("salary",2)over(w2))
      //数据分析窗口函数 lead 取后n行数据
      .withColumn("lead",lead("salary",1)over(w2))
      .withColumn("max_salary_range",max("salary")over(w3))
      //.dropDuplicates("dept") //去掉重复部门
      .show()



    sparkSession.stop()


  }
}
