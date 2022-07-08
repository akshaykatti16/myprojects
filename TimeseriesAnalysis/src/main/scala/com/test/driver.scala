package com.test

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._

object driver {

  def clickSessList(tmo: Long): UserDefinedFunction = udf { (uid: String, clickList: Seq[String], tsList: Seq[Long]) =>

    /* +-------+-------------------------------------------------------------------------------------------------------------------+-----------------------+
    |user_id|click_list                                                                                                         |ts_list                |
    +-------+-------------------------------------------------------------------------------------------------------------------+-----------------------+
    |u1     |[2018-01-01 16:30:00.0, 2018-01-01 17:00:00.0, 2018-01-01 17:30:00.0, 2018-01-01 17:45:00.0, 2018-01-01 19:45:00.0]|[0, 1800, 1800, 900, 0]|
      |u2     |[2018-01-01 16:30:00.0, 2018-01-02 16:30:00.0, 2018-01-02 16:45:00.0, 2018-01-02 18:55:00.0]                       |[0, 0, 900, 0]         |
      +-------+-------------------------------------------------------------------------------------------------------------------+-----------------------+*/

    def sid(n: Long) = s"$uid--$n"

    val sessList = /*[0, 1800, 1800, 900, 0]*/ tsList.foldLeft((List[String](), 0L, 0L /*accumulators*/ )) {
      case ((ls, j, k), i /*tsList iterables*/ ) =>
        if (i == 0 || j + i >= tmo) {
          (sid(k + 1) :: ls, 0L, k + 1) //add "uid--sessionNo" to list
        } else (sid(k) :: ls, j + i, k)
    } /*o/p till here "[u1--2,u1--1,u1--1,u1--1,u1--1], 0, 2" */ ._1 /* => [u1--2,u1--1,u1--1,u1--1,u1--1] */ .reverse /*[u1--1,u1--1,u1--1,u1--1,u1--2]*/
    clickList zip sessList
    /* [2018-01-01 16:30:00.0, 2018-01-01 17:00:00.0, 2018-01-01 17:30:00.0, 2018-01-01 17:45:00.0, 2018-01-01 19:45:00.0] zip [u1--1,u1--1,u1--1,u1--1,u1--2] */
    /*+-------------------------------------------------------------------------------------------------------------------------------------------------+
    |click_sess_id                                                                                                                                    |
    +-------------------------------------------------------------------------------------------------------------------------------------------------+
    |[[2018-01-01 16:30:00,u1--1], [2018-01-01 17:00:00,u1--1], [2018-01-01 17:30:00,u1--1], [2018-01-01 17:45:00,u1--1], [2018-01-01 19:45:00,u1--2]]|
    |[[2018-01-01 16:30:00,u2--1], [2018-01-02 16:30:00,u2--2], [2018-01-02 16:45:00,u2--2], [2018-01-02 18:55:00,u2--3]]                             |
    +-------------------------------------------------------------------------------------------------------------------------------------------------+*/
  }
  def processAndAnalyzeTimeseriesdata(spark: SparkSession): Unit = {
    //Read flat file
    import spark.implicits._
    val flatfile = spark.read.textFile("src/main/resources/timeseriesdata.txt")
    val inputdf = flatfile.map(row => {
      val cols = row.split("\t")
      (cols(0), cols(1))
    }).toDF("click_time", "user_id")
    val castedinputdf = inputdf.withColumn("click_time", $"click_time".cast("Timestamp"))
    castedinputdf.show()
    castedinputdf.printSchema()
    //tmo1 session expires after inactive
    val tmo1: Long = 60 * 60
    //tmo2 max elapsed time for active session
    val tmo2: Long = 2 * 60 * 60
    val win1 = Window.partitionBy("user_id").orderBy("click_time")
    //add a new column to calculate time diff betwn 2 clicks for a same user ordered by time
    val df1 = castedinputdf.withColumn(
      "ts_diff_in_min",
      unix_timestamp($"click_time")
        - unix_timestamp(
        lag($"click_time", 1).over(win1)
      )
    ).withColumn("ts_diff_in_min", lit($"ts_diff_in_min"))
    df1.show()
    //if time_diff is >1 hr or if its 1st click , then set time_diff to 0;
    //session expire after 1 hr
    val df2 = df1.withColumn("ts_diff_in_min",
      when(row_number.over(win1) === 1 || $"ts_diff_in_min" >= tmo1, 0L)
        .otherwise($"ts_diff_in_min")
    )
    df2.show()

    val df3 = df2.
      groupBy("user_id").agg(
      collect_list($"click_time").as("click_list"), collect_list($"ts_diff_in_min").as("ts_list")
    )
    df3.show(false)
    df3.printSchema()
    /* +-------+-------------------------------------------------------------------------------------------------------------------+-----------------------+
     |user_id|click_list                                                                                                         |ts_list                |
     +-------+-------------------------------------------------------------------------------------------------------------------+-----------------------+
     |u1     |[2018-01-01 16:30:00.0, 2018-01-01 17:00:00.0, 2018-01-01 17:30:00.0, 2018-01-01 17:45:00.0, 2018-01-01 19:45:00.0]|[0, 1800, 1800, 900, 0]|
       |u2     |[2018-01-01 16:30:00.0, 2018-01-02 16:30:00.0, 2018-01-02 16:45:00.0, 2018-01-02 18:55:00.0]                       |[0, 0, 900, 0]         |
       +-------+-------------------------------------------------------------------------------------------------------------------+-----------------------+*/
    val df4 = df3.
      withColumn("click_sess_id",
        clickSessList(tmo2)($"user_id", $"click_list", $"ts_list")
      )
    df4.select("user_id", "click_sess_id").show(false)
    /*+-------+-------------------------------------------------------------------------------------------------------------------------------------------------+
    |user_id|click_sess_id                                                                                                                                    |
    +-------+-------------------------------------------------------------------------------------------------------------------------------------------------+
    |u1     |[[2018-01-01 16:30:00,u1--1], [2018-01-01 17:00:00,u1--1], [2018-01-01 17:30:00,u1--1], [2018-01-01 17:45:00,u1--1], [2018-01-01 19:45:00,u1--2]]|
    |u2     |[[2018-01-01 16:30:00,u2--1], [2018-01-02 16:30:00,u2--2], [2018-01-02 16:45:00,u2--2], [2018-01-02 18:55:00,u2--3]]                             |
    +-------+-------------------------------------------------------------------------------------------------------------------------------------------------+*/
    val df5 = df4.
      withColumn("click_sess_id",
        explode($"click_sess_id")
      ).select($"user_id", $"click_sess_id._1".as("click_time"), $"click_sess_id._2".as("sess_id"))
    /*+-------+-------------------+-------+
    |user_id|click_time         |sess_id|
    +-------+-------------------+-------+
    |u1     |2018-01-01 16:30:00|u1--1  |
      |u1     |2018-01-01 17:00:00|u1--1  |
      |u1     |2018-01-01 17:30:00|u1--1  |
      |u1     |2018-01-01 17:45:00|u1--1  |
      |u1     |2018-01-01 19:45:00|u1--2  |
      |u2     |2018-01-01 16:30:00|u2--1  |
      |u2     |2018-01-02 16:30:00|u2--2  |
      |u2     |2018-01-02 16:45:00|u2--2  |
      |u2     |2018-01-02 18:55:00|u2--3  |
      +-------+-------------------+-------+*/
    val resDf = df5.withColumn("click_time",$"click_time".cast("timestamp"))
    resDf.show(false)
    resDf.printSchema()

    //Problem statement 3
    //•Get Number of sessions generated in a day.
    resDf.createOrReplaceTempView("sessionedData")
    spark.sql(
      """select date(click_time) as sessdate, count(distinct(sess_id)) as totalSess from  sessionedData
        |group by  sessdate """.stripMargin).show()

    //•	Total time spent by a user in a day
    val tmpdf = spark.sql(
      """ select user_id, click_time,
        |cast(lead(click_time,1,click_time) over (partition by user_id,cast(click_time as date) order by user_id,cast(click_time as date),click_time asc ) as long)
        |- cast(click_time as long) as lead_time
        |from sessionedData""".stripMargin)
    /*
    * +-------+-------------------+---------+
|user_id|click_time         |lead_time|
+-------+-------------------+---------+
|u1     |2018-01-01 16:30:00|1800     |
|u1     |2018-01-01 17:00:00|1800     |
|u1     |2018-01-01 17:30:00|900      |
|u1     |2018-01-01 17:45:00|7200     |
|u1     |2018-01-01 19:45:00|0        |
|u2     |2018-01-02 16:30:00|900      |
|u2     |2018-01-02 16:45:00|7800     |
|u2     |2018-01-02 18:55:00|0        |
|u2     |2018-01-01 16:30:00|0        |
+-------+-------------------+---------+*/
    tmpdf.show(false)
    tmpdf.createOrReplaceTempView("temp")

    spark.sql("select user_id,cast(click_time as date) as sessdate,sum(lead_time)/60 " +
      "  from temp" +
      " group by user_id,sessdate ").show(false)

    // Problem satement 3 : Total time spent by a user over a month.
    spark.sql("select user_id , " +
      "concat(year(cast(click_time as date)),'_',month(cast(click_time as date))) as yearmonth, sum(lead_time)/60 from temp" +
      " group by  user_id, yearmonth ").show()



  }
  def main(args: Array[String]): Unit = {
    //initialize spark session
    val spark = SparkSession.builder().master("local[*]").appName("TimeSeriesAnalysis").getOrCreate()
    LogManager.getRootLogger.setLevel(Level.ERROR)
    processAndAnalyzeTimeseriesdata(spark)
    //Refer tracing of logic -> C:\Users\ak186148\IdeaProjects_backup\TimeseriesAnalysis\src\main\resources\tracing.txt
    spark.stop()
  }
}
