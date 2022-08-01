package kafkasparkexample

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._

object driver {

  def realTimeConsoleAppend(spark: SparkSession,  df: DataFrame) = {

    val personStringDF = df.selectExpr("CAST(value AS STRING)")

    val schema = new StructType()
      .add("id",IntegerType)
      .add("firstname",StringType)
      .add("middlename",StringType)
      .add("lastname",StringType)
      .add("dob_year",IntegerType)
      .add("dob_month",IntegerType)
      .add("gender",StringType)
      .add("salary",IntegerType)

    val personDF = personStringDF.select(from_json(col("value"), schema).as("data"))
      .select("data.*")


    personDF.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
    /*
    * Batch: 0
-------------------------------------------
22/08/01 16:09:12 INFO CodeGenerator: Code generated in 16.5637 ms
22/08/01 16:09:12 INFO CodeGenerator: Code generated in 13.7537 ms
+---+---------+----------+--------+--------+---------+------+------+
| id|firstname|middlename|lastname|dob_year|dob_month|gender|salary|
+---+---------+----------+--------+--------+---------+------+------+
|  1|   James |          |   Smith|    2018|        1|     M|  3000|
|  2| Michael |      Rose|        |    2010|        3|     M|  4000|
|  3|  Robert |          |Williams|    2010|        3|     M|  4000|
+---+---------+----------+--------+--------+---------+------+------+

* Batch: 1
-------------------------------------------
22/08/01 16:10:30 INFO WriteToDataSourceV2Exec: Data source writer org.apache.spark.sql.execution.streaming.sources.MicroBatchWriter@28c91373 committed.
22/08/01 16:10:30 INFO SparkContext: Starting job: start at driver.scala:52
22/08/01 16:10:30 INFO DAGScheduler: Job 3 finished: start at driver.scala:52, took 0.000039 s
22/08/01 16:10:30 INFO CheckpointFileManager: Writing atomically to file:/C:/Users/aksha/AppData/Local/Temp/temporary-c4a4a8be-bda1-426c-862e-9a7c2d81df41/commits/1 using temp file file:/C:/Users/aksha/AppData/Local/Temp/temporary-c4a4a8be-bda1-426c-862e-9a7c2d81df41/commits/.1.0fb33810-3f0f-44c6-87c3-7f7b634d0b9e.tmp
+---+---------+----------+--------+--------+---------+------+------+
| id|firstname|middlename|lastname|dob_year|dob_month|gender|salary|
+---+---------+----------+--------+--------+---------+------+------+
|  4|   Maria |      Anne|   Jones|    2005|        5|     F|  4000|
+---+---------+----------+--------+--------+---------+------+------+

*Batch: 2
-------------------------------------------
+---+---------+----------+--------+--------+---------+------+------+
| id|firstname|middlename|lastname|dob_year|dob_month|gender|salary|
+---+---------+----------+--------+--------+---------+------+------+
|  5|      Jen|      Mary|   Brown|    2010|        7|      |    -1|
+---+---------+----------+--------+--------+---------+------+------+

22/08/01 16:11:23 INFO CheckpointFileManager: Renamed temp file file:/C:/Users/aksha/AppData/Local/Temp/temporary-c4a4a8be-bda1-426c-862e-9a7c2d81df41/commits/.2.63033997-97c4-4b9a-9d0a-120b14ec89d9.tmp to file:/C:/Users/aksha/AppData/Local/Temp/temporary-c4a4a8be-bda1-426c-862e-9a7c2d81df41/commits/2
22/08/01 16:11:23 INFO MicroBatchExecution: Streaming query made progress: {
  "id" : "ed8e64ae-0a9d-4a36-abea-d6b2a44fdab6",
  "runId" : "285599a3-c246-4dd4-85b7-52bede6004a1",
  "name" : null,
  "timestamp" : "2022-08-01T10:41:22.536Z",
  "batchId" : 2,
  "numInputRows" : 1,
  "inputRowsPerSecond" : 62.5,
  "processedRowsPerSecond" : 1.9723865877712032,
  "durationMs" : {
    "addBatch" : 205,
    "getBatch" : 0,
    "getEndOffset" : 0,
    "queryPlanning" : 19,
    "setOffsetRange" : 1,
    "triggerExecution" : 507,
    "walCommit" : 144
  },
  "stateOperators" : [ ],
  "sources" : [ {
    "description" : "KafkaV2[Subscribe[kafka_topic1]]",
    "startOffset" : {
      "kafka_topic1" : {
        "0" : 4
      }
    },
    "endOffset" : {
      "kafka_topic1" : {
        "0" : 5
      }
    },
    "numInputRows" : 1,
    "inputRowsPerSecond" : 62.5,
    "processedRowsPerSecond" : 1.9723865877712032
  } ],
  "sink" : {
    "description" : "org.apache.spark.sql.execution.streaming.ConsoleSinkProvider@4989eb98"
  }
}
22/08/01 16:11:23 INFO Fetcher: [Consumer clientId=consumer-1, groupId=spark-kafka-source-43386f31-8b80-4a38-86d8-7de4348b41b3--1475555870-driver-0]
*  Resetting offset for partition kafka_topic1-0 to offset 5.* */

  }

  def realTimeKafkaTopicAppend(spark: SparkSession, df: DataFrame) = {


    df.writeStream
      .format("kafka")
      .outputMode("append")
      .option("kafka.bootstrap.servers", "AKDell5415:9092")
      .option("topic", "kafka_sink_topic1")
      .option("checkpointLocation", "src/main/resources/checkpoint")
      .start()
      .awaitTermination()
    /*
    * kafka consumer ouptput
    * :\Users\aksha>kafka-console-consumer.bat -bootstrap-server AKDell5415:9092 --topic kafka_sink_topic1
[2022-08-01 16:52:40,365] WARN [Consumer clientId=console-consumer, groupId=console-consumer-62437] Error while fetching metadata with correlation id 2 : {kafka_sink_topic1=LEADER_NOT_AVAILABLE} (org.apache.kafka.clients.NetworkClient)
{"id":1,"firstname":"James ","middlename":"","lastname":"Smith","dob_year":2018,"dob_month":1,"gender":"M","salary":3000}
{"id":2,"firstname":"Michael ","middlename":"Rose","lastname":"","dob_year":2010,"dob_month":3,"gender":"M","salary":4000}
{"id":3,"firstname":"Robert ","middlename":"","lastname":"Williams","dob_year":2010,"dob_month":3,"gender":"M","salary":4000}
{"id":4,"firstname":"Maria ","middlename":"Anne","lastname":"Jones","dob_year":2005,"dob_month":5,"gender":"F","salary":4000}
{"id":5,"firstname":"Jen","middlename":"Mary","lastname":"Brown","dob_year":2010,"dob_month":7,"gender":"","salary":-1}
{"id":6,"firstname":"Jenou","middlename":"Maryium","lastname":"Brownolk","dob_year":2011,"dob_month":7,"gender":"","salary":900}
{"id":6,"firstname":"Jenou","middlename":"Maryium","lastname":"Brownolk","dob_year":2011,"dob_month":7,"gender":"","salary":900}
{"id":7,"firstname":"Jenou","middlename":"Maryium","lastname":"Brownolk","dob_year":2011,"dob_month":7,"gender":"","salary":900}
{"id":8,"firstname":"Jenou","middlename":"Maryium","lastname":"Brownolk","dob_year":2011,"dob_month":7,"gender":"","salary":900}
{"id":9,"firstname":"Jenou","middlename":"Maryium","lastname":"Brownolk","dob_year":2011,"dob_month":7,"gender":"","salary":900}
    *
    * */
  }

  def main(args: Array[String]): Unit = {
    //initialize spark session
    val spark = SparkSession.builder().master("local[*]").appName("sparkStreamingWithKafkaExample").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val df = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "AKDell5415:9092")
      .option("subscribe", "kafka_topic1")
      .option("startingOffsets", "earliest") // From starting
      .load()

    df.printSchema()
    /*
    * root
 |-- key: binary (nullable = true)
 |-- value: binary (nullable = true)
 |-- topic: string (nullable = true)
 |-- partition: integer (nullable = true)
 |-- offset: long (nullable = true)
 |-- timestamp: timestamp (nullable = true)
 |-- timestampType: integer (nullable = true)*/

    //realTimeConsoleAppend(spark,df)

    realTimeKafkaTopicAppend(spark,df)



    spark.stop()
  }
}
