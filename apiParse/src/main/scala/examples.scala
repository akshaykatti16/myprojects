
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, MapType, StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.util.matching.Regex
/*import scala.language.postfixOps*/


//for schema
case class LogCol(time:String, user: String, ip1: String, ip2: String, val1:String, val2:String, val3:String,
                  req1:String, req2: String, val4: String, val5: String, meth:String, apilink:String, protocol:String
                 , browser: String)

object examples {

  val regex: Regex = "^(\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (.*)".r

  def apiLogParse(spark: SparkSession) = {

    import spark.implicits._

    //Read source
    val source = spark.sparkContext.textFile("src/main/resources/elb.log")
    println("source is of type - "+source.getClass)
    println("source.getStorageLevel before persisting"+source.getStorageLevel)
    //source.persist(StorageLevel.MEMORY_ONLY_SER)
    println("source.getStorageLevel after persisting"+source.getStorageLevel)
    //source.unpersist()
/*
* source is of type - class org.apache.spark.rdd.MapPartitionsRDD
source.getStorageLevel before persistingStorageLevel(1 replicas)
source.getStorageLevel after persistingStorageLevel(memory, 1 replicas)
source.getNumPartitions2
source.partitions.size 2
spark.sparkContext.defaultParallelism 16
spark.sql.shuffle.partitions 200
sourcedf type is of - class org.apache.spark.sql.Dataset*/

    println("source.getNumPartitions"+source.getNumPartitions)
    println("source.partitions.size "+source.partitions.size)
    println("spark.sparkContext.defaultParallelism "+spark.sparkContext.defaultParallelism)
    println("spark.sql.shuffle.partitions "+spark.conf.get("spark.sql.shuffle.partitions"))
    source.collect().foreach(println)
    //action 1 | job 0 | 1 stage (read)

    val sourcedf = source.filter(line => line.matches(regex.toString()))
      /*splitting each record of matched ones*/
      .map(record=> record.split(" "))
      /*assign schema*/
      .map(col=>LogCol(col(0),col(1),col(2),col(3),col(4),col(5),col(6),col(7),col(8),col(9),col(10),col(11),col(12),col(13),col(14)))
      .toDF()

    println("sourcedf type is of - "+sourcedf.getClass)
    sourcedf.show(false)
    // action 2 | job 1 | 1 Stage (filter,map,map - all in same stage as they are narrow transformations)

    sourcedf.cache()
    val resdf = sourcedf.select("apilink").groupBy("apilink").agg(count("*").alias("cnt")).sort(col("cnt").desc_nulls_last)
    resdf/*.repartition(3).*/show()
    // action 3 | job 2 | stages 2 (stage 1 -> till groupby  ; stage 2 -> after group by)

    //resdf.cache()
    resdf.collect().foreach(println)
    // action 4 | job 3 | stages 2 (stage 1 -> till grouped result  ; stage 2 -> start collecting to driver)
    /*
    * without caching
    *
Job Id  ▾
Description
Submitted
Duration
Stages: Succeeded/Total	Tasks (for all stages): Succeeded/Total
4
collect at examples.scala:67
2022/09/07 20:27:06	0.7 s	2/2 (1 skipped)
206/206 (2 skipped)
3
collect at examples.scala:67
2022/09/07 20:27:06	0.6 s	2/2
202/202
2
show at examples.scala:63
2022/09/07 20:27:04	1 s	2/2
202/202
1
show at examples.scala:58
2022/09/07 20:27:04	40 ms	1/1
1/1
0
collect at examples.scala:47
2022/09/07 20:27:02	0.2 s	1/1
2/2 */

/* with caching
*
Job Id  ▾
Description
Submitted
Duration
Stages: Succeeded/Total	Tasks (for all stages): Succeeded/Total
4
collect at examples.scala:67
2022/09/07 20:25:48	0.8 s	2/2 (1 skipped)
206/206 (2 skipped)
3
cache at examples.scala:66
2022/09/07 20:25:48	0.6 s	2/2
202/202
2
show at examples.scala:63
2022/09/07 20:25:46	1 s	2/2
202/202
1
show at examples.scala:58
2022/09/07 20:25:46	36 ms	1/1
1/1
0
collect at examples.scala:47
2022/09/07 20:25:43	0.3 s	1/1
2/2*/
    Thread.sleep(300000)
    System.exit(0)

    /*val rdd1 = spark.sparkContext.parallelize(1 to 200,5)
    println("rdd1 numparts "+rdd1.getNumPartitions)*/

    //matching log record with regex
    var res = source.filter(line => line.matches(regex.toString()))
      /*splitting each record of matched ones*/
      .map(record=> record.split(" "))
      /*assign schema*/
      .map(col=>LogCol(col(0),col(1),col(2),col(3),col(4),col(5),col(6),col(7),col(8),col(9),col(10),col(11),col(12),col(13),col(14))).toDF()

    //create table
    res.createOrReplaceTempView("eblLogs")

    //fetching total count as column values for performing percentage calculation
    val tmp = spark.sql(
      s"""
         |
         |select count(apilink) from eblLogs
       """.stripMargin)
    val total = tmp.first().get(0)
    tmp.show(false)

    //extract api only
    val interim1 = spark.sql(
      s"""
         |select apilink,substring_index(substring_index(apilink,'.', 3),"/",-1) as api,$total as totalRows from eblLogs
       """.stripMargin)
    interim1.createOrReplaceTempView("eblLogs_api_extract")
    interim1.show(false)

    //finding aggregrate count per api
    val interm2 = spark.sql(
      s"""
         |
         |select api,totalRows,count(api) as AggrCnt from eblLogs_api_extract
         |group by api,totalRows
         |order by AggrCnt DESC
         |
       """.stripMargin)
    interm2.createOrReplaceTempView("eblLogs_api_report")
    interm2.show(false)

    //finding final result
    val result = spark.sql(
      s"""
         |select api,AggrCnt,((AggrCnt/totalRows)*100)as percentage from eblLogs_api_report
       """.stripMargin)
    result.show(false)

    //write o/p to csv
    //result.coalesce(1).write.format("csv").option("header","true").option("mode","overwrite").save("C:\\Users\\ak186148\\Desktop\\resultSet")

  }
  def subjectWiseAverage(spark: SparkSession): Unit = {

    val inputrdd = spark.sparkContext.parallelize(Seq(
                          ("maths", 50), ("maths", 60),
                          ("english", 65),("english", 65),("english", 63),("english", 64),
                          ("physics", 66), ("physics", 61), ("physics", 87)),
                          /*partition*/ 2)
    println("No of partitions :"+inputrdd.getNumPartitions)
    println("input rdd type "+inputrdd.getClass.toString)
    println("input rdd")
    inputrdd.collect.foreach(println)
    /*
    * 1st Argument : createCombiner is called when a key(in the RDD element) is found for the first time in a given Partition.
    * This method creates an initial value for the accumulator for that key
    2nd Argument : mergeValue is called when the key already has an accumulator
    3rd Argument : mergeCombiners is called when more that one partition has accumulator for the same key*/

    val reduced = inputrdd.combineByKey(
      //assign 1 to mark value
      (mark) => (mark,1) , //50,1 ; 65,1 ; 66,1
      (acc: (Int,Int), v) =>  (acc._1 + v, acc._2 + 1), //50 +60, 1+1 ;
                                                        // 65+62 , 1+1 >> 127+63 , 2+1 >> 190 + 64,3+1
      (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2) // if same key on another partition
    )
    reduced.cache().collect().foreach(println)
    val result = reduced.mapValues(x => x._1 / x._2.toFloat)
    println("subject wise average")
    result.collect().foreach(println)

    /******************/
    println("countByValue :"+inputrdd.countByValue())
    println("collect as map :"+inputrdd.collectAsMap())//non duplicate keys

    println("lookup :"+inputrdd.lookup("physics"))


  }
  def reduceFoldAggregrate(spark: SparkSession) = {

    val rdd1 = spark.sparkContext.parallelize(List(1, 2, 5,89,4,56,4))
    val sum = rdd1.reduce((x, y) => x+ y)
    println("sum of List(1, 2,4,2,,56 5,89,4,56) :"sum)

    /***********************************************************************/

    val Salaries = spark.sparkContext.parallelize(List(("Jill",11.0),("John",21.0),("Jack",1000.0),("Bob",2000.0),("Bob",2500.0),("Carl",7000.0)),5)
    /*println(spark.sparkContext.defaultMinPartitions)
    println(spark.sparkContext.defaultParallelism)
    println("Salaries.getNumPartitions"+Salaries.getNumPartitions)*/

    val dummySal = ("dummy",0.0) //tuple
    /*Syntax
    def fold[T](acc:T)((acc,value) => acc)
    --T is the data type of RDD
    --acc is accumulator of type T which will be return value of the fold operation
    --A function , which will be called for each element in rdd with previous accumulator*/

    val highest = Salaries.fold(dummySal)(
      (acc /* return value of fold*/, emp) => {if (acc._2 < emp._2) emp else acc}
    )
    println("highest sal :"+highest)


    val deptEmployees = spark.sparkContext.parallelize(List(
      ("IT",("Jack",1000.0)), ("IT",("Bob",2000.0)),("HR",("Bob",2500.0)), ("HR",("Carl",7000.0))
    ))

    val maxByDept = deptEmployees.foldByKey(dummySal){
      (acc, emp) => if (acc._2 > emp._2) acc else emp
    }

    println("highest salaries in each dept")
    maxByDept.foreach(println)

    /************************************************************************/

    /*Disadvantage : This disadvantage of both reduce() & fold() is,
    the return type should be the same as the RDD element type.
    NOTE : aggregate() function can be used to avoid this limitation.*/

    //seqop,combop
    val aggresult = Salaries.aggregate(0.0/*add to sum*/,0/*add to partition*/)(
      (acc, emp) => (acc._1 + emp._2, acc._2 + 1),// seq operation per partition
      /* 1000.0,1; 2000.0,1; 7000.0,1 */
      (acc1, acc2) => (acc1._1 +acc2._1, acc1._2 + acc2._2) // comb operation on (seq operation per partition)
      // (operation on aggregated data of all partitions)
      //1000.0+2000.0+7000.0, 1+1+1
    )
    println("aggregrate func (sum,totalElements) of salries  : "+aggresult)

    val aggBykeyResult = deptEmployees.aggregateByKey(0.0)(

      (acc , value) => acc + value._2,
      (acc1, acc2) => acc1 + acc2
    )
    println("aggregrateByKey ")
    aggBykeyResult.collect().foreach(println)


    /************** groupBy and groupByKey **********/
    val res2 = Salaries.groupByKey()
    println("groupByKey")
    res2.foreach(println)

    /*groupBy() can be used in both unpaired & paired RDDs.
    When used with unpaired data, the key for groupBy() is decided by the function literal passed to the method*/
    val res3 = rdd1.groupBy(x => if(x%2 == 0) "even" else "odd")
    println("groupBy")
    res3.foreach(println)


  }
  def cacheAndPersist(spark: SparkSession) = {

    val data = spark.sparkContext.parallelize(1 to 10)
    data.count() //action

    //cache
   /* data.cache()
    data.count()*/

    //persist
    data.persist(StorageLevel.DISK_ONLY)
    data.count()
    data.unpersist()
  }
  def reduceByKeyEx(spark: SparkSession) = {

    val inputrddWithHeader   = spark.sparkContext.textFile("C:\\Users\\ak186148\\OneDrive - Teradata\\Desktop\\elb.log\\emp.csv")
    //Remove header
    val header = inputrddWithHeader.first()
    val input = inputrddWithHeader.filter(x => x != header )

    val mappedData = input.map( x =>
      { val splitted = x.split(",")
        (splitted(0),splitted(1).toInt)
      }
    )
    println("*************mappedData**************")
    mappedData.collect.foreach(println)

    val reducedData = mappedData.reduceByKey((x,y)=>x+y)
    println("************reduced***********")
    reducedData.collect.foreach(println)

    println("rdd lineage (todebugString)")
    println(reducedData.toDebugString)

    /*****************************************************flatmap********/
    val book  = spark.sparkContext.textFile("C:\\Users\\ak186148\\OneDrive - Teradata\\Desktop\\elb.log\\sample.txt")
    val words = book.flatMap(line => line.split(" "))
    val mapped = words.map( x => (x,1))
    println("************mappedData*********")
    mapped.collect.foreach(println)
    val reduce = mapped.reduceByKey((x,y)=>x+y)
    println("********reduced******************")
    reduce.collect.foreach(println)

    /**************mapPartitionsWithIndex**********/
    //distribute book to 3 nodes
    val book2 = book.repartition(3)

    /*
    mapPartitions() can be used as an alternative to map() & foreach().
    mapPartitions() is called once for each Partition unlike map() & foreach() which is called for each element in the RDD.
    The main advantage being that, we can do initialization on Per-Partition basis instead of per-element basis/
    */

    val words2 = book2.flatMap(x => x.split(" ")).filter(x => !(x.isEmpty))
    val mapped2 = words2.mapPartitionsWithIndex{
      // 'iterator' to iterate through all elements
      //                         in the partition
      (index, iterator) => {
        println("Called in Partition -> " + index)
        //perform db operation or any filesystem/connection related operation
        val myList = iterator.toList
        myList.map(x => (x,1) + " inside partition " + index).iterator
      }
    }
    println("*********mapwithpartitionindex************")
    mapped2.foreach(println)
 }
  def namedfunction(a:Int = 1, b:Int = 1):Int = {
    a-b
  }
  def hoFunction(a:Int, f:Int=>AnyVal):Unit = {
    println(f(a))	//Calling that function
  }
  def multiplyBy2(a:Int):Int = {
    a*2
  }
  def loopandSetOpsEx(spark: SparkSession) = {

    val data=spark.sparkContext.parallelize(Seq(("sun",1),("mon",2),("tue",3), ("wed",4),("thus",5)),5 /*noOfPartitions*/)
    println(data.getNumPartitions)
    val r = data.coalesce(1)
    r.foreach(println)
    println(r.getNumPartitions)
    val transData = data.map(x => (x._1.toUpperCase,x._2))
    transData.foreach(println)

    var abresult = namedfunction(a = 15, b = 2) // Params names are passed during call
    var baresult = namedfunction(b = 15, a = 2) // Params order have changed during call
    var defaultresult = namedfunction()
    println(abresult+" "+baresult+" "+defaultresult)
    //higher order
    hoFunction(25, multiplyBy2)

    //anonymous
    var mul = (x: Int, y: Int) => x*y
    println(mul(3, 4))

    //partially applied
    var calculateDiscount = (discount: Double, costprice: Double) => {(1 - discount/100) * costprice} //anonymous
    val discountedPrice = calculateDiscount(25 /*discountRate*/, _: Double /*partially applied*/)
    println("discounted price : "+discountedPrice(400 /*costPrice passed here */))


    val ArrayVal = for (i <- 1 to 10) yield i * 3
    ArrayVal.foreach(println)

    val subArrayVal = for (e <- ArrayVal if e % 2 == 0) yield e
    subArrayVal.foreach(println)
    /************/

    val rdd1 = spark.sparkContext.parallelize(List("lion", "tiger", "tiger", "peacock", "horse"))
    val rdd2 = spark.sparkContext.parallelize(List("lion", "tiger", "giraffe"))
    val res1 =rdd1.distinct().collect()
    println("distinct")
    res1.foreach(println)

    val res2 = rdd1.union(rdd2).collect()
    println("union")
    res2.foreach(println)

   val res3 = rdd1.intersection(rdd2).collect()
   println("intersection")
    res3.foreach(println)

   val res4 = rdd1.subtract(rdd2).collect()
    println("a -b ")
    res4.foreach(println)

    val res5 = rdd1.cartesian(rdd2).collect()
    //rdd1.cartesian(rdd2).coalesce(1).saveAsTextFile("C:\\Users\\ak186148\\OneDrive - Teradata\\Desktop\\res5_cartesian")
    println("cartesian | very expensive for larger rdd")
    res5.foreach(println)

    /* top () and overriding ordering*/
    val inputrdd = spark.sparkContext.parallelize{ Seq(10, 4, 5, 3, 11, 2, 6) }
    val top1 = inputrdd.top(1)
    println("top from 10, 4, 5, 3, 11, 2, 6 :")
    top1.foreach(println)

    implicit val sortIntegersByString = new Ordering[Int] {
      override def compare(a: Int, b: Int) = {
        //a.toString.compare(b.toString)
        if(a > b) {-1} else {+1}
      }
    }

    val top2 = inputrdd.top(1)
    println("top from 10, 4, 5, 3, 11, 2, 6 aftering overriding compare method :")
    top2.foreach(println)

    /***********takeSample -- for subset of rdd******************************************/
    println("take subset of 10, 4, 5, 3, 11, 2, 6" )
    inputrdd.takeSample(true,4,System.nanoTime().toInt).foreach(println)
    println("subset 3")
    inputrdd.takeSample(false, 3, System.nanoTime.toInt).foreach(println)


  }
  def cogroupAndJoins(spark: SparkSession) = {
    val rdd1 = spark.sparkContext.parallelize(Seq(("key1", 1),("key2", 2),("key1", 3)))
    val rdd2 = spark.sparkContext.parallelize(Seq(("key1", 5),("key2", 4),("key3",6)))
    val grouped = rdd1.cogroup(rdd2)
    println("cogrouped")
    grouped.collect().foreach(println)

    val joined = rdd1.join(rdd2)
    println(" inner joined")
    joined.collect().foreach(println)

    val outerjoined = rdd1.fullOuterJoin(rdd2)
    println(" outer joined")
    outerjoined.collect().foreach(println)

    val leftJoined = rdd1.leftOuterJoin(rdd2)
    println("left outer joined")
    leftJoined.collect().foreach(println)

    val rightJoined = rdd1.rightOuterJoin(rdd2)
    println("right outer joined")
    rightJoined.collect().foreach(println)

    //df joins
    import spark.implicits._
    val payment = spark.sparkContext.parallelize(Seq(
      (1, 101,2500), (2,102,1110), (3,103,500), (4 ,104,400), (5 ,105, 150), (6 ,106, 450)
    )).toDF("paymentId", "customerId","amount")
    val customer = spark.sparkContext.parallelize(Seq((101,"Jon") , (102,"Aron") ,(103,"Sam"))).toDF("customerId", "name")
    val innerJoinDf = customer.join(payment,"customerId")
    innerJoinDf.show(false)
    customer.printSchema()
    payment.printSchema()
    innerJoinDf.printSchema()



  }
  def accumulatorBV(spark: SparkSession) = {

    val broadcastVar = spark.sparkContext.broadcast(Array(1, 2, 3))
    println("broadcasted array values")
    broadcastVar.value.foreach(println)

    //accumulator to count blank lines when file is distributed accross diff partitions
    var blankLinesCnt = spark.sparkContext.longAccumulator("BlankLineCounter")
    println("Initial accum val :"+blankLinesCnt.value)

    val file = spark.sparkContext.textFile("C:\\Users\\ak186148\\OneDrive - Teradata\\Desktop\\elb.log\\sample2.txt",4)
    println("file partitions: "+file.getNumPartitions)

    file.map{x =>
         if (x.length() == 0)
           blankLinesCnt.add(1)
    }.collect()
    println("Final blankLinesCnt ::"+blankLinesCnt.value)

    //https://supergloo.com/spark-scala/spark-broadcast-accumulator-examples-scala/
  }
  def explodeExamples(spark: SparkSession) : Unit = {
    import spark.implicits._
    val rowWiseData = Seq(
      Row("James",List("Java","Scala"),Map("hair"->"black","eye"->"brown"),Row("mum","IND")),
      Row("Michael",List("Spark","Java",null),Map("hair"->"brown","eye"->null),Row("pun","IND")),
      Row("Robert",List("CSharp",""),Map("hair"->"red","eye"->""),Row("blr","IND")),
      Row("Washington",null,null,Row("kol","IND")),
      Row("Jefferson",List(),Map(),Row("chn","IND"))
    )

    val schemaFields = new StructType().add("Name",StringType)
      .add("Languages",ArrayType(StringType))
      .add("features",MapType(StringType,StringType))
      .add("address",new StructType()
        .add("city",StringType)
        .add("country",StringType)
      )

    val personDF = spark.createDataFrame(spark.sparkContext.parallelize(rowWiseData),schemaFields)

    //explode – array column example
    // null are not exploded (not included in df also)
    //also displaying single element from complex types
    personDF.show(false)
    personDF.printSchema()

    val singleElementsDF = personDF.select(
      $"Languages".getItem(0).as("FirstLang"),
      $"features".getItem("hair").as("hairColor"),
      personDF("address.country"),personDF("address.city"))
    singleElementsDF.show(false)

    val arrExplodeDf = personDF.select($"Name",explode($"Languages"))
    println("arrExplodeDf")
    arrExplodeDf.show(false)

    //explode_outer – array example
    //Unlike explode, if the array or map is null or empty, explode_outer returns null
    val arrExplodeOuterDf = personDF.select($"Name",explode_outer($"Languages"))
    println("arrExplodeOuterDf")
    arrExplodeOuterDf.show(false)

    //posexplode_outer – array example
    val arrposExplodeOuterDf = personDF.select($"Name",posexplode_outer($"Languages"))
    println("arrposExplodeOuterDf")
    arrposExplodeOuterDf.show(false)


    //explode – map column example
    val mapExplodDf = personDF.select($"Name",explode($"features"))
    println("mapExplodDf")
    mapExplodDf.show(false)

    //explode_outer – map example
    val mapExplodOuterDf = personDF.select($"Name",explode_outer($"features"))
    println("mapExplodOuterDf")
    mapExplodOuterDf.show(false)

    //posexplode_outer – map example
    val mapPosExplodOuterDf = personDF.select($"Name",posexplode_outer($"features"))
    println("mapPosExplodOuterDf")
    mapPosExplodOuterDf.show(false)

    //struct type explode
    val structExplode = personDF.select($"name",$"address.country",$"address.city")
    println("structExplode")
    structExplode.show(false)
  }
  def rank_DenseRnk_RowNum(spark: SparkSession) = {

    import spark.implicits._
    val df = spark.sparkContext.parallelize(Seq(("aks", 28), ("vinod", 30),("vinod", 29),("vinod", 29))).toDF("name","age")
    println("without functions")
    df.show(false)
    val windowSpec = Window.partitionBy("name").orderBy("age")
    println("with functions")
    df.withColumn("rank",rank() over(windowSpec))
      .withColumn("denseRank", dense_rank() over(windowSpec))
        .withColumn("rowNum",row_number() over(windowSpec)).show(false)


  }
  def windowFunctionsEx(spark: SparkSession) = {

    import spark.implicits._
    val customers = spark.sparkContext.parallelize(List(("Alice", "2016-05-01", 50.00),
      ("Alice", "2016-05-03", 45.00),
      ("Alice", "2016-05-04", 55.00),
      ("Bob", "2016-05-01", 25.00),
      ("Bob", "2016-05-04", 29.00),
      ("Bob", "2016-05-06", 27.00))).toDF("name","date","amount")

    println("before any winSpec")
    customers.show(false)

    val winSpec1 = Window.partitionBy("name").orderBy("date").rowsBetween(-1,1)
    println("movingAverage")
    customers.withColumn("movingAverage_winSpec1",avg("amount").over(winSpec1)).orderBy("name","date").show(false)

    //Cumulative sum
    println("Cumulative sum")
    val winSpec2 = Window.partitionBy("name").orderBy("date").rowsBetween(Long.MinValue /*from starting to current*/ ,0)
    println("Cumulative sum")
    customers.withColumn("Cumulative_sum",sum("amount").over(winSpec2)).show(false)

    //use of lag to get value from "n" previous rows
    println("use of lag to get value of nth previous/after rows")
    val winSpec3 = Window.partitionBy("name").orderBy("date")
    customers.withColumn("prevAmountSpent",lag($"amount",2 /*offset --> previous 2 records*/).over(winSpec3))
      .withColumn("aheadAmountSpent",lead($"amount",2 /*offset --> previous 2 records*/).over(winSpec3)).show(false)

    /*+-----+----------+------+---------------+----------------+
    |name |date      |amount|prevAmountSpent|aheadAmountSpent|
    +-----+----------+------+---------------+----------------+
    |Bob  |2016-05-01|25.0  |null           |27.0            |
      |Bob  |2016-05-04|29.0  |null           |null            |
      |Bob  |2016-05-06|27.0  |25.0           |null            |
      |Alice|2016-05-01|50.0  |null           |55.0            |
      |Alice|2016-05-03|45.0  |null           |null            |
      |Alice|2016-05-04|55.0  |50.0           |null            |
      +-----+----------+------+---------------+----------------+*/

    //use of rank to get no of visits
    println("use of rank to get no of visits")
    customers.withColumn("rank",rank().over(winSpec3)).show(false)
    /*  +-----+----------+------+----+
    |name |date      |amount|rank|
    +-----+----------+------+----+
    |Bob  |2016-05-01|25.0  |1   |
      |Bob  |2016-05-04|29.0  |2   |
      |Bob  |2016-05-06|27.0  |3   |
      |Alice|2016-05-01|50.0  |1   |
      |Alice|2016-05-03|45.0  |2   |
      |Alice|2016-05-04|55.0  |3   |
      +-----+----------+------+----+*/

  }
  def jsonExamples(spark: SparkSession) = {
    val df22 = spark.read.format("org.apache.spark.sql.json")
      .option("multiline","true")
      //.json("src/main/resources/multiline-zipcode.json")
      .load("src/main/resources/multiline-zipcode.json")
    //read multiline json file
    df22.show(false)

    val df23 = spark.read.format("org.apache.spark.sql.json")
      .json("src/main/resources/zipcode2.json")
    //read multiline json file
    df23.show(false)

  }
  def pivotAnunpivotDF(spark: SparkSession) : Unit = {

    val data = Seq(("Banana",1000,"USA"), ("Carrots",1500,"USA"), ("Beans",1600,"USA"),
      ("Orange",2000,"USA"),("Orange",2000,"USA"),("Banana",400,"China"),
      ("Carrots",1200,"China"),("Beans",1500,"China"),("Orange",4000,"China"),
      ("Banana",2000,"Canada"),("Carrots",2000,"Canada"),("Beans",2000,"Mexico"))

    import spark.sqlContext.implicits._
    val df = data.toDF("Product","Amount","Country")
    df.show()

    val aggDF = df.groupBy("Product","Country").agg(sum("Amount")).sort("Product","Country")
    aggDF.show()
    /*
    * +-------+-------+-----------+
|Product|Country|sum(Amount)|
+-------+-------+-----------+
| Banana| Canada|       2000|
| Banana|  China|        400|
| Banana|    USA|       1000|
|  Beans|  China|       1500|
|  Beans| Mexico|       2000|
|  Beans|    USA|       1600|
|Carrots| Canada|       2000|
|Carrots|  China|       1200|
|Carrots|    USA|       1500|
| Orange|  China|       4000|
| Orange|    USA|       4000|
+-------+-------+-----------+*/

    //pivot will transpose the countries from DataFrame rows into columns and produces below output.
    // Where ever data is not present, it represents as null by default
    val pivotdf = df.groupBy("Product").pivot("Country").sum("Amount")
    pivotdf.show()
    /*
    * +-------+------+-----+------+----+
|Product|Canada|China|Mexico| USA|
+-------+------+-----+------+----+
| Orange|  null| 4000|  null|4000|
|  Beans|  null| 1500|  2000|1600|
| Banana|  2000|  400|  null|1000|
|Carrots|  2000| 1200|  null|1500|
+-------+------+-----+------+----+*/

    //Selective pivot
    val selectedcountries = Seq("USA","India")
    val pivotDF2 = df.groupBy("Product").pivot("Country",selectedcountries).sum("Amount")
    pivotDF2.show()
    /*
    * +-------+----+-----+
|Product| USA|India|
+-------+----+-----+
| Orange|4000| null|
|  Beans|1600| null|
| Banana|1000| null|
|Carrots|1500| null|
+-------+----+-----+*/
    //unpivot using stack()
    val unPivotdf = pivotdf.select($"Product",
      expr("stack(4, 'canada'" /*row val*/ +
        ", Canada, 'China', China, 'Mexico', Mexico, 'USA', USA) " +
        "as (Country,Total)")
    )
      //.where("Total is not null")
    unPivotdf.na.drop("any").show()
    /*|Product|Country|Total|
    +-------+-------+-----+
    | Orange|  China| 4000|
      | Orange|    USA| 4000|
      |  Beans|  China| 1500|
      |  Beans| Mexico| 2000|
      |  Beans|    USA| 1600|
      | Banana| canada| 2000|
      | Banana|  China|  400|
      | Banana|    USA| 1000|
      |Carrots| canada| 2000|
      |Carrots|  China| 1200|
      |Carrots|    USA| 1500|
      +-------+-------+-----+*/


  }
  def CDCexamplebySpark(spark: SparkSession) : Unit = {
    import spark.implicits._
    val basedf = spark.read.options(Map("header" -> "true","delimiter" -> ",","inferSchema" -> "true"))
      .csv("src/main/resources/basedataCDC.csv")
      //change date format of lastupdatedate
      .withColumn("lastupdatedate",
        date_format(unix_timestamp(trim(col("lastupdatedate")), "MM-dd-yyyy").cast("timestamp"), "yyyy-MM-dd")
      )
      /* Add default date where null */.na.fill("0001-01-01",Seq("lastupdatedate"))
    basedf.show()
    basedf.printSchema()
   /* +------+--------------+---------+
    |prodid|lastupdatedate|indicator|
    +------+--------------+---------+
    |     1|    0001-01-01|        A|
      |     2|    1981-01-25|        A|
      |     3|    1982-01-26|        A|
      |     4|    1985-12-20|        A|
      +------+--------------+---------+*/

    val updateddf = spark.read.options(Map("header" -> "true","delimiter" -> ",","inferSchema" -> "true"))
      .csv("src/main/resources/updatedataCDC.csv")
      .withColumn("lastupdatedate",
        date_format(unix_timestamp(trim(col("lastupdatedate")), "MM-dd-yyyy").cast("timestamp"), "yyyy-MM-dd")
      )
    updateddf.show()
    updateddf.printSchema()
    /*+------+--------------+---------+
    |prodid|lastupdatedate|indicator|
    +------+--------------+---------+
    |     2|    2018-01-25|        U|
      |     4|    2018-01-25|        U|
      |     6|    2018-01-25|        A|
      |     8|    2018-01-25|        A|
      +------+--------------+---------+*/

    //join both data frame
    val tmpdf = basedf.join(updateddf,Seq("prodid"),"outer").sort("prodid")
    tmpdf.show()
    /*+------+--------------+---------+--------------+---------+
|prodid|lastupdatedate|indicator|lastupdatedate|indicator|
+------+--------------+---------+--------------+---------+
|     1|    0001-01-01|        A|          null|     null|
|     2|    1981-01-25|        A|    2018-01-25|        U|
|     3|    1982-01-26|        A|          null|     null|
|     4|    1985-12-20|        A|    2018-01-25|        U|
|     6|          null|     null|    2018-01-25|        A|
|     8|          null|     null|    2018-01-25|        A|
+------+--------------+---------+--------------+---------+*/

    val tempdf = basedf.as("table1").join(updateddf.as("table2"),Seq("prodid"), "outer")
    .select(col("prodid"),
      /*Set lastupdatedate
      When base table's lastupdatedate isNull, then its a new record to be inserted, then set "lastupdatedate" in result table as that of update table
      Otherwise set "lastupdatedate" as that of base table as it will be existing record*/
        when(col("table1.lastupdatedate").isNotNull, col("table1.lastupdatedate"))
          .otherwise(col("table2.lastupdatedate")).as("lastupdatedate"),
      /* Set changeDate
      When base table's indicator isNOTNull, then its existing record
         And When update table's lastupdatedate isNotNull, then there is update to existing record, set "changeDate" as update table's lastupdatedate
        Otherwise set "changeDate" as 9999-12-31 as it will be new record*/
        when(col("table1.indicator").isNotNull,
          when(col("table2.lastupdatedate").isNotNull, col("table2.lastupdatedate"))
            .otherwise(lit("9999-12-31"))
        ).otherwise(lit("9999-12-31")).as("changeDate"),
      /* Set indicator
      * When update table's indicator isNULL , then no updates to base table record, set "indicator" as that of base table
      *  Otherwise When update table's indicator is "U", set "indicator" to "I"
      *   Otherwise set "indicator" as that of update table
      * */
        when(col("table2.indicator").isNull, col("table1.indicator"))
          .otherwise(when(col("table2.indicator") === "U", lit("I"))
            .otherwise(col("table2.indicator"))).as("indicator"))
      .sort("prodid")
    /*
    * +------+--------------+----------+---------+
|prodid|lastupdatedate|changeDate|indicator|
+------+--------------+----------+---------+
|     1|    0001-01-01|9999-12-31|        A|
|     2|    1981-01-25|2018-01-25|        I|
|     3|    1982-01-26|9999-12-31|        A|
|     4|    1985-12-20|2018-01-25|        I|
|     6|    2018-01-25|9999-12-31|        A|
|     8|    2018-01-25|9999-12-31|        A|
+------+--------------+----------+---------+*/

    tempdf.show()
    //filtering tempdf for duplication
    val filtereddf = tempdf.filter(col("indicator") === "I")
      .withColumn("lastupdatedate", col("changeDate"))
      .withColumn("changeDate", lit("9999-12-31"))
      .withColumn("indicator", lit("A"))

    //finally merging both dataframes
    tempdf.union(filtereddf).sort("prodid", "lastupdatedate").show(false)



  }

  def main(args: Array[String]): Unit = {

    //initialize spark session
    val spark = SparkSession.builder().master("local[*]").appName("AdvancedSparkExamples").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    //Ex1 : API wise log parser | dataframes
    apiLogParse(spark)

    //Ex2 : average in each subject using combineByKey(), mapValues()
    // also countByKey(), collectAsMap(),  lookup()
    //subjectWiseAverage(spark)

    //Ex3 : reduce() for sum of values,cntByVal , fold() for highest/lowest salary , aggregrate() for (total,num of partitions)
    //groupBy and groupBykey
    //fold() and foldByKey
    //aggregrate() and aggregrateByKey()
    //reduceFoldAggregrate(spark)

    //Ex4 : cache and persist()
    //cacheAndPersist(spark)

    //Ex5 : map()=> for structured data, flatMap() => for flat file(txt >> wordcount), csv file >> reduceByKey()
    //also mapPartitionswithIndex() , toDebugString (rdd lineage)
    //reduceByKeyEx(spark)

    //Ex6: forloop, yield, saveAsTextFile
    /*distinct(), union(), intersection(), subtract(), cartesian()
    * override ordering for top(),
    * takeSample*/
    //loopandSetOpsEx(spark)

    //Ex7: cogroup,join, etc
    //cogroupAndJoins(spark)

    //Ex8: BV, Accumulator, etc
    //accumulatorBV(spark)

    //Ex9: explode examples
    //explodeExamples(spark)

    //Ex10: rank, dense rank and row_number
    //rank_DenseRnk_RowNum(spark)

    //Ex 11 : Spark Window Functions
    //windowFunctionsEx(spark)
    //https://www.geeksforgeeks.org/program-to-convert-java-set-to-sequence-in-scala/?ref=rp
    //https://www.geeksforgeeks.org/program-to-convert-java-set-to-list-in-scala/?ref=rp
    //https://www.geeksforgeeks.org/program-to-print-java-set-of-strings-in-scala/?ref=rp
    //https://www.geeksforgeeks.org/program-to-convert-java-list-to-set-in-scala/?ref=rp
    // all under https://www.geeksforgeeks.org/scala-and-java-interoperability/?ref=rp

    //Ex 12 : Read and write json
    //jsonExamples(spark)

    //Ex 13 : pivot an unpivot
    //pivotAnunpivotDF(spark)

    //Ex 14 : Spark CDC project
    //CDCexamplebySpark(spark)


    spark.stop()

    }

}
