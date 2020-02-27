package method

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.SparkStrategies
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.expressions.scalalang.typed

object select {
  def main(args: Array[String]): Unit = {

    //1. 构建Spark SQL中核心对象SparkSession
    val sparkSession = SparkSession.builder().appName("wordcount").master("local[*]").getOrCreate()

    // select ... from 表明 where 过滤条件 group by 分组字段 having 筛选条件 order by 排序字段 asc|desc limit 限制返回几条结果

    //m2(sparkSession)
    //m3(sparkSession)
    //m4(sparkSession)
    //m5(sparkSession)
    //m6(sparkSession)
    //m7(sparkSession)
    //m8(sparkSession)
    //m9(sparkSession)
    //m10(sparkSession)
    m11(sparkSession)


    /**
      * 窗口函数 案例2：JD热卖榜统计(每个分类下商品的热销榜)
      *
      * @param spark
      */
    def m11(spark: SparkSession): Unit = {
      val rdd = spark.sparkContext.makeRDD(
        List(
          (101, "2018-01-01", "洗衣机", "海尔", 1, 999),
          (102, "2018-01-01", "洗衣机", "海尔", 1, 999),
          (103, "2018-01-01", "洗衣机", "小天鹅", 1, 999),
          (104, "2018-01-01", "洗衣机", "西门子", 1, 999),
          (105, "2018-01-01", "手机", "iphone", 1, 999)
        ))
     import spark.implicits._

      val df = rdd.toDF("order_id","create_time","category","product","num","price")



    import org.apache.spark.sql.functions._
      df
          .selectExpr("category","product")
          .groupBy("category","product")
          .count()
          .withColumn("rank",rank()over(Window
                                                    .partitionBy("category")
                                                    .orderBy($"count"desc)))
          .where("rank <= 1")
          .show()

      spark.stop()
    }


    /**
      * 窗口函数 案例1：统计每个用户访问页面次数最多的前10个
      *
      * @param spark
      */
    def m10(spark: SparkSession): Unit = {
      val rdd = spark.sparkContext.makeRDD(
        List(
          ("2018-01-01", 1, "www.baidu.com", "10:01"),
          ("2018-01-01", 2, "www.baidu.com", "10:01"),
          ("2018-01-01", 1, "www.sina.com", "10:01"),
          ("2018-01-01", 3, "www.baidu.com", "10:01"),
          ("2018-01-01", 3, "www.baidu.com", "10:01"),
          ("2018-01-01", 1, "www.sina.com", "10:01")
        ))
      import spark.implicits._
      val df = rdd.toDF("day", "user_Id", "page_id", "time")
      // 1. 每个用户访问不同页面的次数
      // select user_id,page_id,count(page_id) from t_log group by user_id,page_id

      // 窗口函数声明
      val w1 = Window
                    .partitionBy("user_id") //将user_id相同的数据划分到同一个窗口中
                    .orderBy($"count"desc)

      import org.apache.spark.sql.functions._

      df
        .selectExpr("user_Id", "page_id")
        .groupBy("user_id", "page_id")
        .count()
        .withColumn("rank",rank()over(w1))  //添加一个列,用以描述排名信息
        .where("rank <= 1")   //只查每个用户访问排名第一的
        .show()

      spark.stop()
    }

    /**
      * na null值处理方法
      *
      * @param spark
      */
    def m9(spark: SparkSession): Unit = {
      import spark.implicits._
      val df = List(
        (1, "math", 85),
        (1, "chinese", 80),
        (1, "english", 90),
        (1, "english", 99),
        (2, "math", 90),
        (2, "chinese", 80)
      ).toDF("id", "course", "score")
      df
        .groupBy("id")
        .pivot("course") // 将课程字段math、chinese、english转换化结果表的字段
        .max("score") // 保留每个科目分数最高的结果
        //.na.fill(-1,Array[String]("english")) // null 只对english字段的null填充一个初始值
        .na.drop(Array[String]("math"))  // null english字段中含有null值则删除这一行记录
        .show()

      spark.stop()
    }


    // 如何通过spark sql实现行转列的处理呢？
    /**
      * pivot 透视语法
      *
      * @param spark
      */
    // 方法一：case ...when...语句
    /*select
    id,
    max(case course when 'math' then score else 0 end) as math,
    max(case course when 'chinese' then score else 0 end) as chinese,
    max(case course when 'english' then score else 0 end) as english
      from
    t_score
    group by id*/
    def m8(spark: SparkSession): Unit = {
      import spark.implicits._
      val df = List(
        (1, "math", 85),
        (1, "chinese", 80),
        (1, "english", 90),
        (1, "english", 99),
        (2, "math", 90),
        (2, "chinese", 80)
      ).toDF("id", "course", "score")

      /*df
        .selectExpr(
          "id",
          "case course when 'math' then score else 0 end as math",
          "case course when 'chinese' then score else 0 end as chinese",
          "case course when 'english' then score else 0 end as english"
        )
        .groupBy("id")
        .max("math","chinese","english")
        .show()*/
      //方法二  pivot透视方法 简化行转列的处理   汉语：以...为中心
      df
          .groupBy("id")
          .pivot("course")  // 将课程字段math、chinese、english转换化结果表的字段
          .max("score")   // 保留每个科目分数最高的结果
          .show()

      spark.stop()
    }



    //agg  聚合方法 强类型
    def m7(spark: SparkSession): Unit = {
      import spark.implicits._
      val df = List(
        (1, "zs", false, 1, 15000),
        (2, "ls", false, 1, 18000),
        (3, "ww", true, 2, 19000),
        (4, "zl", false, 1, 18000)
      ).toDF("id", "name", "sex", "dept", "salary")

      df
        //.groupByKey(row => row.getInt(3)) // 3是第四列的下标
        // 聚合操作支持：count sum avg 三种
        //.agg(typed.sumLong(row => row.getInt(4)))

        .groupBy("dept")
        .sum("salary")
        .show()

      spark.stop()
    }

    //limit 限制返回的结果条数
    def m6(spark: SparkSession): Unit = {
      import spark.implicits._
      val df = List(
        (1, "zs", false, 1, 15000),
        (2, "ls", false, 1, 18000),
        (3, "ww", true, 2, 19000),
        (4, "zl", false, 1, 18000)
      ).toDF("id", "name", "sex", "dept", "salary")

      df
        .orderBy($"salary" desc)
        .limit(3) // 用户中工资最高的前三个人
        .show()

      spark.stop()
    }

    //groupBy 分组方法，将内容相同的数据分为一组
    def m5(spark: SparkSession): Unit = {
      import spark.implicits._
      val df = List(
        (1, "zs", false, 1, 15000),
        (2, "ls", false, 1, 18000),
        (3, "ww", true, 2, 19000),
        (4, "zl", false, 1, 18000)
      ).toDF("id", "name", "sex", "dept", "salary")

      df
        // 统计不同部门员工的最高工资 select dept,max(salary) from t_user group by dept
        .groupBy($"dept")
        .max("salary")
        .where("dept = 2") // 等价于having
        .show()

      spark.stop()
    }

    //where  条件过滤方法
    def m4(spark: SparkSession): Unit = {
      import spark.implicits._
      val df = List(
        (1, "zs", false, 1, 15000),
        (2, "ls", false, 1, 18000),
        (3, "ww", true, 1, 19000),
        (4, "zl", false, 1, 18000)
      ).toDF("id", "name", "sex", "dept", "salary")

      df
        .where("name='zs' or salary > 18000")
        // === 类似于JS判断语法  值和类型
        //.where($"name" === "zs")
        .show()

      spark.stop()
    }

    //orderBy | sort    结果排序
    def m3(sparkSession: SparkSession)={
      import sparkSession.implicits._
      val df = List(
        (1, "zs", false, 1, 15000),
        (2, "ls", false, 1, 18000),
        (3, "ww", true, 1, 19000),
        (4, "zl", false, 1, 18000)
      ).toDF("id", "name", "sex", "dept", "salary")

      df
        // 根据salary和id进行结果降序排列 (依次排序 如果第一个列内容相同再根据第二个列的内容排序)
        //.orderBy($"salary" desc,$"id" desc)
        .sort($"salary"desc,$"id"desc)
        .show()

      sparkSession.stop()
    }


    // dropDuplicates去重复  相当于db distinct
    def m2(sparkSession: SparkSession)={
      import sparkSession.implicits._
      val df = List(
        (1, "zs", false, 1, 15000),
        (2, "ls", false, 1, 18000),
        (3, "ww", true, 1, 19000),
        (4, "zl", false, 1, 18000)
      ).toDF("id", "name", "sex", "dept", "salary")

      df
        .dropDuplicates("sex") // 去重复  相当于db distinct
        .show()

      sparkSession.stop()
    }


    def m1(sparkSession: SparkSession) ={
      //2. 通过spark session对象构建dataset或者dataframe
      val rdd = sparkSession.sparkContext.makeRDD(List((1, "zs", true,2000), (2, "ls", false,3000)))

      import sparkSession.implicits._
      //给每一个列起别名
      //val df = rdd.toDF()
      val df = rdd.toDF("id","name","sex","salary")

      df
        //.select("_1")
        //.select("id","name") //select 投影查询，指定查询的字段

        //.selectExpr("id+10","name as username")   //支持表达式（基本运算或者别名）的投影查询

        // select id,name,sex,salary,salary * 12 as  yearsalary from t_user
        .withColumn("year_salary",$"salary" * 12)  //添加额外列方法

        .withColumnRenamed("name","username") //给列重命名方法  相当于as 别名

        .drop($"salary")   //用来删除特定列方法    等价于：select id,name,sex,salary*12 as year_salary from t_user

        .show()
      //.printSchema()  //打印输出表的结构  相当于Navicat中的desc
    }

    sparkSession.stop()


  }
}
