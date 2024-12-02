package proj.flightdata.secondpass.four

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}
import proj.common.UDFDefs._
import proj.common.{FlightData, PassengersFlownTogether}

import java.sql.Date

object App extends App {

  private val spark = SparkSession
    .builder().master("local")
    .appName("flight-data-assignment-four")
    .config("spark.eventLog.enabled", value = true)
    .config("spark.eventLog.dir", "/Users/kevinlawrence/Downloads/spark-history-server/eventLogs/")
    .getOrCreate()

  private val flightDataDf = spark.
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
    repartitionByRange(col("passengerId"))

  val result = flightDataDf.as("t1").join(
      broadcast(flightDataDf).as("t2"),
      Seq("flightId"),
      "outer"
    ).selectExpr(
      "flightId",
      "t1.passengerId as p1",
      "t2.passengerId as p2",
      "t1.date as date"
    ).where("p1 <> p2").
    withColumn("hash", createHashUdf(col("p1"), col("p2"))).
    withColumn("row_num", row_number().over(Window.partitionBy(col("hash")).orderBy(col("p1")))).
    withColumn("kakidsFids", collect_set("flightId").over(Window.partitionBy(col("hash")).orderBy(col("p1")))).
    withColumn("date", collect_set("date").over(Window.partitionBy(col("hash")).orderBy(col("p1")))).
    filter(col("row_num") === 1). // remove duplicates of tuples of passengerId
    withColumn("zip_col", explode(arrays_zip(col("kakidsFids"), col("date")))).
    selectExpr("p1", "p2", "zip_col.kakidsFids as flightId", "zip_col.date as date")

}
