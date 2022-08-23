package example

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object driver {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[*]")
      .config("hive.metastore.uris","thrift://localhost:9083")
      /*.config("hive.exec.scratchdir","/tmp/mydir")*/
      .config("spark.sql.warehouse.dir","hdfs://localhost:9000/user/hive/warehouse")
      .appName("sparkHiveSupportApp")
      .enableHiveSupport()
      .getOrCreate()

    //Read plain text data in df and write to hive tables

    val df1 = spark.read.option("inferSchema","true").option("header","true").csv("src/main/resources/test1.csv")
    spark.sql("create database if not exists database1").write.mode(SaveMode.Overwrite).saveAsTable("database1.table1")

    val df2 = spark.read.option("inferSchema","true").option("header","true").csv("src/main/resources/test2.csv")
    spark.sql("create database if not exists database2")
    df2.write.mode(SaveMode.Overwrite).saveAsTable("database2.table2")

    val tblist = List("database1.table1","database2.table2")
    var trgTable = spark.emptyDataFrame
    var dbTbllist = Seq[DataFrame]()

    for(tbl <- tblist){
      //table() is a dataframe
      val srctbl = spark.table(tbl)
      dbTbllist=dbTbllist:+srctbl // append all tables from tables list of strings to tables sequence of dataframes
    }

    if(dbTbllist.length>1){
      trgTable = dbTbllist.reduce(_ union _)
    }

    spark.sql("create database if not exists database3")
    trgTable.distinct.write.mode(SaveMode.Overwrite).saveAsTable("database3.table3")

   /*
   * hive> show databases;
database1
database2
database3
default
emp
temp
hive> use database1;
hive> show tables;
table1
2022-08-23 12:32:39,309 INFO session.SessionState: Resetting thread name to  main
hive> select * from table1;
2022-08-23 12:37:07,669 INFO ql.Driver: Semantic Analysis Completed (retrial = false)
2022-08-23 12:37:07,670 INFO ql.Driver: Returning Hive schema: Schema(
fieldSchemas:
[FieldSchema(name:table1.name, type:string, comment:null), FieldSchema(name:table1.salary, type:double, comment:null), FieldSchema(name:table1.dob, type:timestamp, comment:null), FieldSchema(name:table1.active, type:boolean, comment:null)],
properties:null)
akshay1 2000.0  1991-06-15 00:00:00     true
akshay2 2001.0  1991-06-16 00:00:00     true
akshay3 2002.0  1991-06-17 00:00:00     false
hive> select * from database2.table2;
shweta1 3000.0  2001-06-15 00:00:00     false
shweta2 3001.0  2001-06-16 00:00:00     true
shweta3 3002.0  2001-06-17 00:00:00     false
Time taken: 0.491 seconds, Fetched: 3 row(s)
hive> select * from database3.table3;
2022-08-23 12:41:45,221 INFO exec.ListSinkOperator: RECORDS_OUT_INTERMEDIATE:0, RECORDS_OUT_OPERATOR_LIST_SINK_3:6,
akshay1 2000.0  1991-06-15 00:00:00     true
shweta1 3000.0  2001-06-15 00:00:00     false
shweta2 3001.0  2001-06-16 00:00:00     true
akshay2 2001.0  1991-06-16 00:00:00     true
akshay3 2002.0  1991-06-17 00:00:00     false
shweta3 3002.0  2001-06-17 00:00:00     false*/

    //Read plain text data in df, create hive table using spark-sql

    import spark.implicits._
    import spark.sql

    sql("CREATE TABLE IF NOT EXISTS emp.sparkEmpl " +
      "( id int, name string, age int, gender string ) " +
      "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' " +
      "WITH SERDEPROPERTIES ( \"separatorChar\" = \",\", \"quoteChar\"  = \"\\'\" ) ")
    sql("LOAD DATA LOCAL INPATH 'src/main/resources/data.csv' INTO TABLE emp.sparkEmpl ")

    sql("SELECT * FROM  emp.sparkEmpl ").show()
    /*
    * +---+--------------+---+------+
| id|          name|age|gender|
+---+--------------+---+------+
|  1|    aks, Katti| 31|  Male|
|  2|  swt, Mahajan| 30|FeMale|
|  3|swt2, Mahajan3| 30|FeMale|
+---+--------------+---+------+*/

    //read hive table on spark as df, then write that df to diff table on hive
    spark.table("emp.sparkempl")
      .write.mode(SaveMode.Overwrite).saveAsTable("emp.sparkempl_copy")

/*
    *>hive select * from emp.sparkempl_copy
     * 2022-08-23 16:21:33,589 INFO exec.ListSinkOperator: RECORDS_OUT_INTERMEDIATE:0, RECORDS_OUT_OPERATOR_LIST_SINK_3:3,
1       aks, Katti      31      Male
2       swt, Mahajan    30      FeMale
3       swt2, Mahajan3  30      FeMale
*/

    //create hive table on top of existing data

    val dataDir = "hdfs://localhost:9000/tmp/aks"
    spark.range(1,20).write.parquet(dataDir)
    sql(s"CREATE EXTERNAL TABLE emp.hive_ints(key int) STORED AS PARQUET LOCATION '$dataDir'")
    spark.read.table(" emp.hive_ints").show()
    val df4 = spark.read.parquet("hdfs://localhost:9000/tmp/aks/*.parquet").show()

    /* 22/08/23 16:38:18 INFO DAGScheduler: Job 3 finished: show at driver.scala:119, took 0.136019 s
+---+
| id|
+---+
|  6|
|  7|
| 12|
| 13|
| 18|
| 19|
|  1|
|  2|
|  3|
|  4|
|  5|
|  8|
|  9|
| 10|
| 11|
| 14|
| 15|
| 16|
| 17|
+---+*/
    //set hive partitioning
    spark.sqlContext.setConf("hive.exec.dynamic.partition", "true")
    spark.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    spark.table("emp.sparkempl")
      .write
      .partitionBy("gender")
      .mode(SaveMode.Overwrite).saveAsTable("emp.sparkemplprttntbl")


    spark.stop()
  }

}
