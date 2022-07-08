import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

case class LogRecord(clientIp: String, clientIdentity: String, user: String, dateTime: String,dateTimeExt: String, requestMethod:String,requestlink:String,
                     request: String, statusCode:String, bytesSent:String, referer:String, userAgent:String )

object logParse {
  val regex = """^([0-9]+.{1}[0-9]+.{1}[0-9]+.{1}[0-9]+) (\S+) (\S+) (\S*) (\S*) (\S*) (\S*) (\S*) (\S*) (\S*) (\S*) (.*)""".r

  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder().master("local[*]").appName("logparse").getOrCreate()
    import spark.implicits._

    val src = spark.sparkContext.textFile("C:\\Users\\ak186148\\IdeaProjects\\logParse\\src\\main\\resources\\test.log")
    val regex = """^([0-9]+.{1}[0-9]+.{1}[0-9]+.{1}[0-9]+) (\S+) (\S+) (\S*) (\S*) (\S*) (\S*) (\S*) (\S*) (\S*) (.*)""".r
    val df = src.filter(line=>line.matches(regex.toString()))
              .map(row=>row.split(" "))
                .map(field=> LogRecord(field(0),field(1),field(2),field(3),field(4),field(5),field(6),field(7),field(8)
                ,field(9),field(10),field(11))).toDF()
    df.createOrReplaceTempView("test")
    val res = spark.sql(
      s"""
         |select distinct(clientIp) from test
       """.stripMargin)
    res.show()

  }
}
