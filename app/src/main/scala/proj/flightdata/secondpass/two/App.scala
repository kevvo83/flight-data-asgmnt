package proj.flightdata.secondpass.two

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}


object App extends App {

  private val spark = SparkSession
    .builder().master("local")
    .appName("flight-data-assignment-secondpass-two")
    .config("spark.eventLog.enabled", value = true)
    .config("spark.eventLog.dir", "/Users/kevinlawrence/Downloads/spark-history-server/eventLogs/")
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

  val passengersDf = spark.
    read.
    option("header", "true").
    schema(
      new StructType().
        add("passengerId", IntegerType).
        add("firstName", StringType).
        add("lastName", StringType)
    ).
    csv("src/main/resources/passengers.csv")

  flightDataDf.as("t1").selectExpr("t1.passengerId", "t1.flightId").
    repartitionByRange(col("passengerId")).
    groupBy(col("passengerId")).
    agg(countDistinct(col("flightId")).alias("number_of_flights")).
    orderBy(desc("number_of_flights"), asc("passengerId")).
    join(
      passengersDf.as("t2"),
      Seq("passengerId"),
      "left"
    ).
    selectExpr(
      "t1.passengerId as passengerId",
      "number_of_flights as number_of_flights",
      "t2.firstName as firstName",
      "t2.lastName as lastName"
    ).
    repartition(1).
    write.option("header", "true").
    mode(SaveMode.Overwrite).format("csv").save("../secondpass/answer2.csv")
}
