package proj.flightdata.four

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{col, row_number}
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructType}
import org.scalatest.funsuite.AnyFunSuite
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner
import org.apache.spark.sql.expressions.Window
import proj.common.FlightData
import proj.common.UDFDefs.createHashUdf
import proj.flightdata.four.App.flightDataDf

@RunWith(classOf[JUnitRunner])
class DataTest extends AnyFunSuite {

  val spark: SparkSession = SparkSession
    .builder().master("local")
    .appName("flight-data-assignment-four")
    .getOrCreate()

  import spark.implicits._

  val flightDataDf: Dataset[FlightData] = spark.
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

  val result: Dataset[Row] = flightDataDf.filter(col("flightId") === 0).as("t1").join(
      flightDataDf.filter(col("flightId") === 0).as("t2"),
      Seq("flightId"),
      "outer"
    ).select(col("flightId"), col("t1.passengerId").as("p1"), col("t2.passengerId").as("p2")).
    where(col("p1") =!= col("p2")).
    withColumn("hash", createHashUdf(col("p1"), col("p2"))).
    withColumn("row_num", row_number().over(Window.partitionBy(col("hash")).orderBy(col("p1")))).
    filter(col("row_num") === 1)

    test("The total number of combinations of passengers who flew together on Flight ID 0 is 100C2 = 4950") {
      assert (result.count() == 4950)
    }

}