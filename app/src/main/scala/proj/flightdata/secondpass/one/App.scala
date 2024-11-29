package proj.flightdata.secondpass.one

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}

object App extends App {

  private val spark = SparkSession
    .builder().master("local")
    .appName("flight-data-assignment-secondpass-one")
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
    withColumn("monthOfYear", month(col("date")))

  flightDataDf.
    repartitionByRange(col("monthOfYear")).
    groupBy(col("monthOfYear")).agg(countDistinct(col("flightId")).alias("number_of_flights")).
    sortWithinPartitions(asc("monthOfYear")). // even orderBy() does the sort locally
    repartition(1).
    write.option("header", "true").
    mode(SaveMode.Overwrite).format("csv").save("../secondpass/answer1.csv")
}
