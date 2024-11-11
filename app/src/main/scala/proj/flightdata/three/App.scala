package proj.flightdata.three

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{asc, col, collect_list, lag, lead, lit, udf}
import org.apache.spark.sql.types.{ArrayType, DateType, IntegerType, StringType, StructType}
import proj.common.FlightData

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

  def getRunsAvoidingUk = udf((s: Array[String]) => getRuns(s, "uk"))

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

  // TODO: Need to fix casting issues with UDF on columns
  flightDataDf.groupBy(col("passengerId")).agg(collect_list(col("from")).alias("locations")).printSchema()
    //withColumn("spans", getRunsAvoidingUk(col("locations"))).show()

}
