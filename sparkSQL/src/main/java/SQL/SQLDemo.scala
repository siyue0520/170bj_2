package SQL

import org.apache.spark.sql.SparkSession

/*sql语法：
    select * from 表名 where 过滤条件 group by 分组字段 having 筛选条件 order by 排序字段 limit 限制返回条数*/
/**
  * 纯SQL语法
  */
object SQLDemo {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("sql operation").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val rdd = spark
      .sparkContext
      .makeRDD(List(
        (1, "zs", true, 1, 15000),
        (2, "ls", false, 2, 18000),
        (3, "ww", false, 2, 14000),
        (4, "zl", false, 1, 18000),
        (5, "win7", false, 1, 18000),
        (5, "win7", false, 5, 18000)
      ))
    import spark.implicits._
    val df = rdd.toDF("id", "name", "sex", "dept", "salary")
    // df起别名 视图名
    // 全局视图(跨多个spark session会话) + 临时视图(只能在创建它的spark session中使用,不能跨会话)
    // 创建一个临时视图 t_user
    df.createTempView("t_user")
    // 如果创建的是全局视图，表会存放到 global_temp.t_user  默认default.xxx
    // df.createOrReplaceGlobalTempView("t_user")

    spark
      //like 模糊查询
      //.sql("select * from t_user where name like '%s%' order by Salary desc")

      // 分组 group by
      //.sql("select dept,avg(salary) from t_user group by dept")

      // 分组后结果过滤  having
      //.sql("select dept,avg(salary) from t_user group by dept having dept = 2")

      // 限制返回的结果条数 limit
      //.sql("select * from t_user order by salary desc limit 3")

      //==================================   高阶语法   =====================================================

      //case...when...
      //.sql("select id,name,case sex when true then '男' else '女' end as sex,dept,salary from t_user")

      // pivot 透视  行转列
    import spark.implicits._
    val df2 = spark.sparkContext.makeRDD(List(
      (1, "math", 85),
      (1, "chinese", 80),
      (1, "english", 90),
      (1, "english", 99),
      (2, "math", 90),
      (2, "chinese", 80)
    )).toDF("id", "course", "score")

    df2.createOrReplaceTempView("t_score")

      /*spark.sql(
        """
          |select
          |   id,
          |   max(case course when 'math' then score else 0 end) as math,
          |   max(case course when 'chinese' then score else 0 end) as chinese,
          |   max(case course when 'english' then score else 0 end) as english
          |from
          |    t_score
          |group by
          |    id
        """.stripMargin)*/
    // pivot(聚合函数 for 行转列的字段 in(最终结果表中列名))
      /*spark.sql(
        """
          |select * from t_score pivot(max(score) for course in('math','chinese','english'))
          |where english is not null
        """.stripMargin)*/


    // 高阶语法：窗口函数
    // 统计每个用户访问次数前十的页面
    val rdd3 = spark.sparkContext.makeRDD(
      List(
        ("2018-01-01", 1, "www.baidu.com", "10:01"),
        ("2018-01-01", 2, "www.baidu.com", "10:01"),
        ("2018-01-01", 1, "www.sina.com", "10:01"),
        ("2018-01-01", 3, "www.baidu.com", "10:01"),
        ("2018-01-01", 3, "www.baidu.com", "10:01"),
        ("2018-01-01", 3, "null", "10:01"),
        ("2018-01-01", 1, "www.sina.com", "10:01"),
        ("2018-01-01", 1, "null", null),
        (null, 1, "null", "10:01")
      ))
    import spark.implicits._
    val df3 = rdd3.toDF("day", "user_id", "page_id", "time")

    df3.createOrReplaceTempView("t_log")

      // 窗口函数sql语法：
      // 窗口函数名() over(partition by 划分窗口字段 order by 窗口内的排序规则 rows between start and end)

      spark.sql(
        """
          | select
          |   *
          | from
          |   (select
          |     user_id,
          |     page_id,
          |     num,
          |     rank() over (partition by user_id order by num desc) as rank
          |    from
          |       (select
          |         user_id,
          |         page_id,
          |         count(page_id) as num
          |        from t_log
          |        group by
          |         user_id,page_id
          |        )
          |   )
          | where rank <= 10
        """.stripMargin
      /*"select user_id,page_id,count(page_id) from t_log group by user_id,page_id"*/
      )
      .show()


    // 案列2：查询用户的基本信息和用户所在部门的平均工资
    // rows between(0,Long_MAX_Value)
    spark
        .sql(
          """
            |select
            |   id,
            |   name,
            |   sex,
            |   dept,
            |   salary,
            |   avg(salary) over(partition by dept order by salary desc rows between unbounded preceding and unbounded following) as avg_salary
            |from
            |   t_user
          """.stripMargin)
        .show()

    //高阶语法，表连接查询
    val rdd4 = spark.sparkContext.makeRDD(List((1,"研发部"),(2,"市场部"),(3,"后勤部")))

    val df4 = rdd4.toDF("dept_id","dept_name")
    df4.createOrReplaceTempView("t_dept")

    spark
      .sql(
      //内连接
        //"select * from t_user t1 inner join t_dept t2 on t1.dept = t2.dept_id"
      //左外连接
        //"select * from t_user t1 left outer join t_dept t2 on t1.dept = t2.dept_id"
      //右外连接
        //"select * from t_user t1 right outer join t_dept t2 on t1.dept = t2.dept_id"
      //全外连接
        //"select * from t_user t1 full outer join t_dept t2 on t1.dept = t2.dept_id"
      //左半开连接 left semi  类似于枚举查询 in 使用右表去过滤左表
        //"select * from t_user t1 left semi join t_dept t2 on t1.dept = t2.dept_id"
      // anti 左半开连接的相反过程
        //"select * from t_user t1 left anti join t_dept t2 on t1.dept = t2.dept_id"
      //cross 笛卡尔乘积
        "select * from t_user t1 cross join t_dept t2 on t1.dept = t2.dept_id"
      )
      .show()

    //cube 多维度分组
    val df5 = spark
      .sparkContext
      .makeRDD(List(
        (110, 50, 80, 80),
        (120, 60, 95, 75),
        (120, 50, 96, 70)))
      .toDF("height", "weight", "IQ", "EQ")

    df5.createOrReplaceTempView("t_info")

    spark
      .sql("select height,weight,avg(IQ),avg(EQ) from t_info group by cube(height,weight)")
      .show()

    spark.stop()




  }

}
