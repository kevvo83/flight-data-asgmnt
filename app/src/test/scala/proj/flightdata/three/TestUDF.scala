package proj.flightdata.three

import org.scalatest.funsuite.AnyFunSuite
import proj.common.UDFDefs.getRuns
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TestUDF extends AnyFunSuite {

  test("A list of 9 destinations with UK appearing at position 3 should return 2 spans of 2 and 6") {

    val testData: Array[String] = Array("tk", "ch", "uk", "fr", "sg", "cg", "dk", "jo", "th")

    val result: Array[Int] = getRuns(testData, "uk")

    assert(result.length == 2)
    assert(result.head == 2 && (result.tail sameElements Array(6)))
  }

}
