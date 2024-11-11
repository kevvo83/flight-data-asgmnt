package proj.flightdata.three

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{asc, col, collect_list, lag, lead, lit, udf}
import org.apache.spark.sql.types.{ArrayType, DateType, IntegerType, StringType, StructType}
import proj.common.FlightData
import proj.common.UDFDefs._

object App extends App {

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
  flightDataDf.groupBy(col("passengerId")).agg(collect_list(col("from")).alias("locations")).
    withColumn("spans", getRunsAvoidingUk(col("locations"))).show()

}
