package proj.flightdata.three

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{array, asc, col, collect_list, desc, flatten}
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructType}
import proj.common.FlightData
import proj.common.UDFDefs._

object App extends App {

  val spark = SparkSession
    .builder().master("local")
    .appName("flight-data-assignment-four")
    .config("spark.eventLog.enabled", value = true)
    .config("spark.eventLog.dir", "../spark-history-server/eventLogs/")
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

  private val table_name = "flight_data_part_by_pass_id"

  val res = flightDataDf.
    sortWithinPartitions(asc("date")).
    withColumn("countries", array(col("from"), col("to"))).
    groupBy(col("passengerId")).agg(collect_list(col("countries")).alias("locations")).
    withColumn("locations", flatten(col("locations"))).
    withColumn("locations", getUniqueLocations(col("locations"))).
    withColumn("spans", getRunsAvoidingUk(col("locations"))).
    withColumn("longestRun", getMaxSpanFromListOfSpans(col("spans")))

  res.
    withColumn("spans", col("spans").cast(StringType)).
    withColumn("locations", col("locations").cast(StringType)).
    orderBy(desc("longestRun")).
    repartition(1).
    write.option("header", "true").
    mode(SaveMode.Overwrite).
    format("csv").save("../answer3.csv")

  res.printSchema()
  res.show(20)
  res.explain()

}
