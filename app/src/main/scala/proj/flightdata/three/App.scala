package proj.flightdata.three

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{asc, col, collect_list, lag, lead, month, udf}
import org.apache.spark.sql.types.{ArrayType, DateType, IntegerType, StringType, StructType}
import java.sql.Date


object App extends App {

  def getRuns(destinations: Array[String], avoid: String): Array[Int] = {
    var res: Array[Int] = Array()
    var acc: Int = 0

    destinations.foreach {
      case "uk" => {
        res = res :+ acc
        acc = 0
      }
      case _ => acc += 1
    }
    res :+ acc
  }

  case class FlightData(passengerId: Integer, flightId: Integer, from: String, to: String, date: Date)

  val spark = SparkSession
    .builder().master("local")
    .appName("flight-data-assignment-four")
    .getOrCreate()

  import spark.implicits._

  val flightDataDf = spark.
    read.
    option("header", "true").
    schema(
      new StructType().
        add("passengerId", IntegerType).
        add("flightId", IntegerType).
        add("from", StringType).
        add("to", StringType).
        add("date", DateType)
    ).
    csv("src/main/resources/flightData.csv").
    repartition(col("passengerId")).as[FlightData]

  flightDataDf.groupBy(col("passengerId")).agg(collect_list(col("from")).alias("locations"))

    flightDataDf.groupBy(col("passengerId")).agg(collect_list(col("from")).alias("locations").cast(StringType)).repartition(1).
      write.option("header", "true").
      mode(SaveMode.Overwrite).
      format("csv").save("../answer3.csv")

}
