package proj.flightdata.four

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, month}
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructType}
import proj.flightdata.oneandtwo.App.spark

object App extends App {

  val spark = SparkSession
    .builder().master("local")
    .appName("flight-data-assignment-four")
    .getOrCreate()

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
    withColumn("monthOfYear", month(col("date")))

  flightDataDf.join()


}
