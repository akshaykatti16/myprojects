package com.thoughtworks.assignment.unsolved

import com.thoughtworks.assignment.SparkSessionBuilder
import org.apache.spark.sql.expressions.Window

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object OutputWriter {
  def write(outDF: DataFrame, path: String)= {
    outDF.coalesce(1).write.option("header",true).csv(path)
  }
}

case class MovingAverage(sparkSession: SparkSession, stockPriceInputDir: String, size: Integer) {
  def calculate() : DataFrame = {
//    calculate moving average
    import sparkSession.implicits._

    //should apply custom schema instead of infer schema option, since inferSchema option is used it fails for dirty
    // data hence explicit cast is used for stockPrice to convert string null into double null so that we could
    //apply notNull filter for dirty data cleanup
    val mvAvgDF=sparkSession.read.option("header",true).option("inferSchema",true).csv(stockPriceInputDir)
      .withColumn("stockPrice",$"stockPrice".cast("double")).filter($"stockPrice" isNotNull)

    //specifying window specification, considering preceding rows till current row to calculate moving average
    //window minus states preceding rows and plus states following rows
    val windowSpec=Window.partitionBy("stockId").orderBy("timeStamp").rowsBetween(-(size-1),0)

//    inorder to pass the test case, the rows which does not have specified preceding rows needs to be avoided
//    for moving average calculations hence additional counter column is added to remove those rows.
//    Calculating average only when current row has specified number of preceding rows and rounding off the result
//    upto 2 decimal places.
    val resultDF= mvAvgDF.withColumn("cntr",count($"stockId") over(windowSpec)).
      withColumn("moving_average",round (
        when($"cntr"=== size,avg($"stockPrice") over(windowSpec)),2)
      )
      .filter($"moving_average" isNotNull)
        .drop("cntr")
      resultDF.orderBy("stockId")
  }
}


object MovingAverage {
  def main(args: Array[String]): Unit = {
    val spark:SparkSession = SparkSessionBuilder.build
    if (args.length != 3) {
      println("Correct Usage[MovingAverage <size> <stockPriceInputDir> <outputDir>]")
    }
    val outputDF = MovingAverage(spark, args(1), Integer.parseInt(args(0))).calculate()
    OutputWriter.write(outputDF, args(2))
 }

}

case class MovingAverageWithStockInfo(sparkSession: SparkSession, stockPriceInputDir: String, stockInfoInputDir: String, size: Integer) {
  def calculate() : DataFrame = {
    import sparkSession.implicits._

//    since moving average is already calculated we just have to invoke the logic for given input stocks
//    and then perform join in between
    val movingAverageDF=MovingAverage(sparkSession,stockPriceInputDir,size).calculate()
    val stockInfoDF=sparkSession.read.option("header",true).option("inferSchema",true).csv(stockInfoInputDir)
    val joinedDF=movingAverageDF.join(stockInfoDF,movingAverageDF("stockId") === stockInfoDF("stockId")).drop(stockInfoDF("stockId"))
      .select("stockId","timeStamp","stockPrice","moving_average","stockName","stockCategory")
     joinedDF
  }
  def calculateForAStock(stockId: String) : DataFrame = {
    import sparkSession.implicits._
    val joinedMovingAverageDF=MovingAverageWithStockInfo(sparkSession,stockPriceInputDir,stockInfoInputDir,size).calculate()
    joinedMovingAverageDF.filter($"stockId" === stockId)
  }
}

object MovingAverageWithStockInfo {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSessionBuilder.build
    if (args.length >= 4) {
      println("Correct Usage[MovingAverage <size> <stockPriceInputDir> <stockInfoInputDir> <outputDir> [<stockId>]]")
    }

    val movingAverageWithStockInfo = MovingAverageWithStockInfo(spark, args(1), args(2), Integer.parseInt(args(0)))

    val outputDF = args.lift(4) match {
      case Some(stockId) => movingAverageWithStockInfo.calculateForAStock(stockId)
      case _ => movingAverageWithStockInfo.calculate()
    }
    OutputWriter.write(outputDF, args(3))
  }
}