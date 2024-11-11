package proj.common

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import java.sql.Date

object UDFDefs {

  private def createHash(ip1: Int, ip2: Int): String =
    if (ip1 < ip2) ip1.toString + ip2.toString
    else ip2.toString + ip1.toString

  val createHashUdf: UserDefinedFunction = udf((ip1: Int, ip2: Int) => createHash(ip1, ip2))

  def flownTogether(details: PassengersFlownTogether)(atLeastNTimes: Int, from: Date, to: Date): Boolean =
    details.numberOfFlightsTogether >= atLeastNTimes && details.from.after(from) && details.to.before(to)

  def getRuns(destinations: Array[String], avoid: String): Array[Int] = {
    var res: Array[Int] = Array()
    var acc: Int = 0

    destinations.foreach {
      case "uk" => {
        res = res :+ acc
        acc = 0
      }
      case _ => acc += 1
    }
    res :+ acc
  }

  def getRunsAvoidingUk: UserDefinedFunction = udf((s: Array[String]) => getRuns(s, "uk"))

}
