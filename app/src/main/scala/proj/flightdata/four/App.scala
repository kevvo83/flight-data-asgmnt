package proj.flightdata.four

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, collect_list, collect_set, count, countDistinct, desc, max, min, row_number, explode}
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructType}
import proj.common.{FlightData, PassengersFlownTogether}
import org.apache.spark.sql.expressions.Window
import proj.common.UDFDefs._

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
    filter(col("row_num") === 1). // remove duplicates of tuples of passengerId
    select(
      col("p1"), col("p2"), col("date"), explode(col("kakidsFids")).as("flightId")
    )

  result.show(20)

  val aggDs = result.groupBy(
      col("p1").as("passengerId1"),
      col("p2").as("passengerId2")).
    agg(
      count("flightId").as("numberOfFlightsTogether"),
      collect_set(col("flightId")).as("flightsFlownTogether"),
      min(col("date")).as("from"),
      max(col("date")).as("to")
    ).orderBy("numberOfFlightsTogether").as[PassengersFlownTogether]

  aggDs.
    //filter(flownTogether(_)(3, Date.valueOf("2017-01-01"), Date.valueOf("2017-12-01"))).
    repartition(1).write.option("header","true").mode(SaveMode.Overwrite).csv("../answer4.csv")


  // result.count()

  // flightDataDf.groupBy('flightId).agg(collect_set('passengerId))

  /*val ds1 = flightDataDf.select($"passengerId".alias("pid2"), $"flightId".alias("fid2"))

  val result = flightDataDf.join(ds1, col("flightId") === col("fid2"), "cross").
    filter(col("passengerId") =!= col("pid2")).
    groupBy(col("passengerId"), col("pid2")).
    agg(
      sumDistinct(col("flightId")).alias("numberOfFlightsTogether"),
      min(col("date")).alias("from"),
      max(col("date")).alias("to")
    ).orderBy(desc("numberOfFlightsTogether"))

  result.filter(col("numberOfFlightsTogether") > 3).repartition(1).write.option("header", "true").
  mode(SaveMode.Overwrite).csv("../answer4.csv")
   */
}
