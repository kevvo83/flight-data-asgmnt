package proj.flightdata.submission.four

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}
import proj.common.UDFDefs._
import proj.common.{FlightData, PassengersFlownTogether}

import java.sql.Date

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

  val result = flightDataDf.as("t1").join(
      flightDataDf.as("t2"),
      Seq("flightId"),
      "outer"
    ).select(
      col("flightId"),
      col("t1.passengerId").as("p1"),
      col("t2.passengerId").as("p2"),
      col("t1.date").as("date")
    ).
    where(col("p1") =!= col("p2")).
    withColumn("hash", createHashUdf(col("p1"), col("p2"))).
    withColumn("row_num", row_number().over(Window.partitionBy(col("hash")).orderBy(col("p1")))).
    withColumn("kakidsFids", collect_set("flightId").over(Window.partitionBy(col("hash")).orderBy(col("p1")))).
    withColumn("date", collect_set("date").over(Window.partitionBy(col("hash")).orderBy(col("p1")))).
    filter(col("row_num") === 1). // remove duplicates of tuples of passengerId
    withColumn("zip_col", explode(arrays_zip(col("kakidsFids"), col("date")))).
    select(
      col("p1"), col("p2"), col("zip_col.kakidsFids").as("flightId"), col("zip_col.date").as("date")
    )

  // result.printSchema()
  result.show(20)

  val aggDs = result.groupBy(
      col("p1").as("passengerId1"),
      col("p2").as("passengerId2")).
    agg(
      count("flightId").as("numberOfFlightsTogether"),
      collect_set(col("flightId")).as("flightsFlownTogether"),
      min(col("date")).as("from"),
      max(col("date")).as("to")
    ).orderBy(desc("numberOfFlightsTogether")).as[PassengersFlownTogether]

  aggDs.
    filter(flownTogether(_)(3, Date.valueOf("2017-10-01"), Date.valueOf("2017-10-31"))).
    withColumn("flightsFlownTogether", col("flightsFlownTogether").cast(StringType)).
    repartition(1).write.option("header","true").mode(SaveMode.Overwrite).csv("../answer4.csv")
}
