package proj.flightdata.secondpass.three

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}
import proj.common.FlightData
import proj.common.UDFDefs._

object App extends App {

  private val spark = SparkSession
    .builder().master("local")
    .appName("flight-data-assignment-secondpass-three")
    .config("spark.eventLog.enabled", value = true)
    .config("spark.eventLog.dir", "/Users/kevinlawrence/Downloads/spark-history-server/eventLogs/")
    .getOrCreate()

  // Major changes:
  // 1. range partition on the column for groupBy - have more partitions on the executor
  // 2. Brings the number of exchanges down

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
    csv("src/main/resources/flightData.csv") // removed the cast to case class - doesn't add much value

  val res = flightDataDf.
    repartition(col("passengerId")).
    sortWithinPartitions(asc("date")).
    withColumn("countries", array(col("from"), col("to"))).
    groupBy(col("passengerId")).agg(collect_list(col("countries")).alias("locations")).
    withColumn("locations", getUniqueLocations(flatten(col("locations")))).
    withColumn("spans", getRunsAvoidingUk(col("locations"))).
    withColumn("longestRun", getMaxSpanFromListOfSpans(col("spans")))

  // Only thing I changed was to remove the cast to `FlightData` case class in DataSet
  // Reason being - I was always using Df operations - this didn't add any value

  res.
    withColumn("spans", col("spans").cast(StringType)).
    withColumn("locations", col("locations").cast(StringType)).
    orderBy(desc("longestRun")).
    repartition(1).
    write.option("header", "true").
    mode(SaveMode.Overwrite).
    format("csv").save("../secondpass/answer3.csv")

}
