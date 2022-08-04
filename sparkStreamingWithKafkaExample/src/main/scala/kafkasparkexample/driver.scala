package kafkasparkexample

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.avro._
import java.nio.file.{Files, Paths}

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

  def realTimeKafkaAvroTypeProducerSink(spark: SparkSession, df: DataFrame) = {


    //Deffine schema for json topic data
    val schema = new StructType()
      .add("id",IntegerType)
      .add("firstname",StringType)
      .add("middlename",StringType)
      .add("lastname",StringType)
      .add("dob_year",IntegerType)
      .add("dob_month",IntegerType)
      .add("gender",StringType)
      .add("salary",IntegerType)
    //apply json and convert to DF
    val personDF = df.selectExpr("CAST(value AS STRING)") // First convert binary to string
      .select(from_json(col("value"), schema).as("data"))
    //convert DF to avro type and Write to new topic
    personDF.select(to_avro(struct("data.*")) as "value")
      .writeStream
      .format("kafka")
      .outputMode("append")
      .option("kafka.bootstrap.servers", "AKDell5415:9092")
      .option("topic", "kafka_avro_sink_topic1")
      .option("checkpointLocation","src/main/resources/checkpointAvro")
      .start()
      .awaitTermination()

    /*Binary avro data written to kafka topic
    * C:\Users\aksha>kafka-console-consumer.bat -bootstrap-server AKDell5415:9092 --topic kafka_avro_sink_topic1
[2022-08-02 18:24:09,965] WARN [Consumer clientId=console-consumer, groupId=console-consumer-24206] Error while fetching metadata with correlation id 2 : {kafka_avro_sink_topic1=LEADER_NOT_AVAILABLE} (org.apache.kafka.clients.NetworkClient)
 ☻ ♀James
Smith ─▼ ☻ ☻M ≡.
 ♦ ►MichaelRose   ┤▼ ♠ ☻M └>
 ♠*/

  }

  def realTimeKafkaAvroTypeConsumerSource(spark: SparkSession, df2: DataFrame) = {


    val jsonFormatSchema = new String(
      Files.readAllBytes(Paths.get("./src/main/resources/person.avsc")
      )
    )

    val personDF2 = df2.select(from_avro(col("value"), jsonFormatSchema).as("person"))
      .select("person.*")
    personDF2.printSchema()
    /*root
 |-- id: integer (nullable = true)
 |-- firstname: string (nullable = true)
 |-- middlename: string (nullable = true)
 |-- lastname: string (nullable = true)
 |-- dob_year: integer (nullable = true)
 |-- dob_month: integer (nullable = true)
 |-- gender: string (nullable = true)
 |-- salary: integer (nullable = true)
    * */

    personDF2.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()

    /*
    * Batch: 0
-------------------------------------------
+---+---------+----------+--------+--------+---------+------+------+
| id|firstname|middlename|lastname|dob_year|dob_month|gender|salary|
+---+---------+----------+--------+--------+---------+------+------+
|  1|   James |          |   Smith|    2018|        1|     M|  3000|
|  2| Michael |      Rose|        |    2010|        3|     M|  4000|
|  3|  Robert |          |Williams|    2010|        3|     M|  4000|
|  4|   Maria |      Anne|   Jones|    2005|        5|     F|  4000|
|  5|      Jen|      Mary|   Brown|    2010|        7|      |    -1|
|  6|    Jenou|   Maryium|Brownolk|    2011|        7|      |   900|
|  6|    Jenou|   Maryium|Brownolk|    2011|        7|      |   900|
|  7|    Jenou|   Maryium|Brownolk|    2011|        7|      |   900|
|  8|    Jenou|   Maryium|Brownolk|    2011|        7|      |   900|
|  9|    Jenou|   Maryium|Brownolk|    2011|        7|      |   900|
| 11|   James |          |   Smith|    2018|        1|     M|  3000|
| 12|   James |          |   Smith|    2018|        1|     M|  3000|
| 13|   James |          |   Smith|    2018|        1|     M|  3000|
| 14|   James |          |   Smith|    2018|        1|     M|  3000|
| 15|   James |          |   Smith|    2018|        1|     M|  3000|
| 16|   Shweta|          |   Smith|    2018|        1|     M|  3000|*/
  }

  def readFromHDFS(spark: SparkSession) = {

    //Different types of output modes
    //outputMode("append")    -> when you want to output only new rows to the output sink
    //outputMode("complete")  -> when you want to aggregate the data and output the entire results to sink every time
    //outputMode("update")    -> outputs the updated aggregated results every time to data sink when new data arrives.
    //                            but not the entire aggregated results like complete mode.
    //                            If the streaming data is not aggregated then, it will act as outputMode("append")

    // a single .json file has multiple rows within, so schema is defined as List
    val schema = StructType(
      List(
        StructField("RecordNumber", IntegerType, true),
        StructField("Zipcode", StringType, true),
        StructField("ZipCodeType", StringType, true),
        StructField("City", StringType, true),
        StructField("State", StringType, true),
        StructField("LocationType", StringType, true),
        StructField("Lat", StringType, true),
        StructField("Long", StringType, true),
        StructField("Xaxis", StringType, true),
        StructField("Yaxis", StringType, true),
        StructField("Zaxis", StringType, true),
        StructField("WorldRegion", StringType, true),
        StructField("Country", StringType, true),
        StructField("LocationText", StringType, true),
        StructField("Location", StringType, true),
        StructField("Decommisioned", StringType, true)
      )
    )

    val df = spark.readStream.schema(schema).json("hdfs://localhost:9000/user/aksha/hdfstest")
    val groupDF = df.select("Zipcode")
      .groupBy("Zipcode").count()
    groupDF.printSchema()

    groupDF.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()

    df.printSchema()

    /*
    * -------------------------------------------
Batch: 0
-------------------------------------------
+-------+-----+
|Zipcode|count|
+-------+-----+
|  76166|    1|
|  32564|    1|
|  85210|    1|
|    709|    1|
|  32046|    1|
|  34445|    1|
|    704|    3|
|  34487|    1|
|  85209|    1|
|  76177|    2|
+-------+-----+
===========================after adding some more .json files to folder
-------------------------------------------
Batch: 1
-------------------------------------------
+-------+-----+
|Zipcode|count|
+-------+-----+
|  76166|    1|
|  32564|    1|
|  85210|    1|
|  36275|    2|
|    709|    1|
|  35146|    2|
|    708|    1|
|  35585|    2|
|  32046|    1|
|  27203|    2|
|  34445|    1|
|  27007|    2|
|    704|    3|
|  27204|    2|
|  85209|    1|
|  34487|    1|
|  76177|    2|*/


  }

  def produceKafkaMessage(spark: SparkSession) = {

    val data = Seq (("iphone", "2007"),("iphone 3G","2008"),
      ("iphone 3GS","2009"),
      ("iphone 4","2010"),
      ("iphone 4S","2011"),
      ("iphone 5","2012"),
      ("iphone 8","2014"),
      ("iphone 10","2017"))

    val df = spark.createDataFrame(data).toDF("key","value")
    df.write
      .format("kafka")
      .option("kafka.bootstrap.servers","AKDell5415:9092")
      .option("topic","text_topic1")
      .save()

  }

  def consumeKafkaMessage(spark: SparkSession) = {

    val df = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", "AKDell5415:9092")
      .option("subscribe", "text_topic1")
      .load()

    df.printSchema()
    df.show(false)
    /*+-------------------------------+-------------+-----------+---------+------+-----------------------+-------------+
|key                            |value        |topic      |partition|offset|timestamp              |timestampType|
+-------------------------------+-------------+-----------+---------+------+-----------------------+-------------+
|[69 70 68 6F 6E 65 20 33 47]   |[32 30 30 38]|text_topic1|0        |0     |2022-08-04 17:01:03.481|0            |
|[69 70 68 6F 6E 65 20 35]      |[32 30 31 32]|text_topic1|0        |1     |2022-08-04 17:01:03.481|0            |
|[69 70 68 6F 6E 65 20 33 47 53]|[32 30 30 39]|text_topic1|0        |2     |2022-08-04 17:01:03.481|0            |
|[69 70 68 6F 6E 65 20 31 30]   |[32 30 31 37]|text_topic1|0        |3     |2022-08-04 17:01:03.481|0            |
|[69 70 68 6F 6E 65 20 38]      |[32 30 31 34]|text_topic1|0        |4     |2022-08-04 17:01:03.481|0            |
|[69 70 68 6F 6E 65 20 34 53]   |[32 30 31 31]|text_topic1|0        |5     |2022-08-04 17:01:03.481|0            |
|[69 70 68 6F 6E 65 20 34]      |[32 30 31 30]|text_topic1|0        |6     |2022-08-04 17:01:03.481|0            |
|[69 70 68 6F 6E 65]            |[32 30 30 37]|text_topic1|0        |7     |2022-08-04 17:01:03.481|0            |
+-------------------------------+-------------+-----------+---------+------+-----------------------+-------------+*/

    val df2 = df.selectExpr("CAST(key AS STRING)",
      "CAST(value AS STRING)","topic")
    df2.show(false)
    /*+----------+-----+-----------+
|key       |value|topic      |
+----------+-----+-----------+
|iphone 3G |2008 |text_topic1|
|iphone 5  |2012 |text_topic1|
|iphone 3GS|2009 |text_topic1|
|iphone 10 |2017 |text_topic1|
|iphone 8  |2014 |text_topic1|
|iphone 4S |2011 |text_topic1|
|iphone 4  |2010 |text_topic1|
|iphone    |2007 |text_topic1|
+----------+-----+-----------+
    * */

  }

  def readKafkaAsBatchProcess(spark: SparkSession) = {

    //Spark SQL Batch Processing – Producing Messages to Kafka Topic.
    //produceKafkaMessage(spark)
    consumeKafkaMessage(spark)

  }

  def main(args: Array[String]): Unit = {
    //initialize spark session
    val spark = SparkSession.builder().master("local[*]").appName("sparkStreamingWithKafkaExample").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    //Read data from Kafka json topic

    /*val df = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "AKDell5415:9092")
      .option("subscribe", "kafka_topic1")
      .option("startingOffsets", "earliest")
      .load()

    df.printSchema()*/

    /*
    * root
 |-- key: binary (nullable = true)
 |-- value: binary (nullable = true)
 |-- topic: string (nullable = true)
 |-- partition: integer (nullable = true)
 |-- offset: long (nullable = true)
 |-- timestamp: timestamp (nullable = true)
 |-- timestampType: integer (nullable = true)*/

    //Read from Kafka json topic and display on console
    //realTimeConsoleAppend(spark,df)

    //Read from Kafka json topic and write to another Kafka topic
    //realTimeKafkaTopicAppend(spark,df)

    //Read from Kafka json topic and write to another Kafka topic in avro format
    //realTimeKafkaAvroTypeProducerSink(spark,df)

    //Read Kafka topic in avro format and display data on console
    /*val df2 = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "AKDell5415:9092")
      .option("subscribe", "kafka_avro_sink_topic1")
      .option("startingOffsets", "earliest")
      .load()
    df2.printSchema()*/

    /*root
 |-- key: binary (nullable = true)
 |-- value: binary (nullable = true)
 |-- topic: string (nullable = true)
 |-- partition: integer (nullable = true)
 |-- offset: long (nullable = true)
 |-- timestamp: timestamp (nullable = true)
 |-- timestampType: integer (nullable = true)*/
    //realTimeKafkaAvroTypeConsumerSource(spark,df2)

    //read json from a hdfs folder and write aggregrated data to console in complete ouptut mode
    //readFromHDFS(spark)
    
    //Read kafka source and batch process with spark sql
    readKafkaAsBatchProcess(spark)

    spark.stop()
  }
}
