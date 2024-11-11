package proj.common

import java.sql.Date

case class PassengersFlownTogether(
                                  passengerId1: Int,
                                  passengerId2: Int,
                                  numberOfFlightsTogether: BigInt,
                                  flightsFlownTogether: Array[Int],
                                  from: Date,
                                  to: Date
                                  )
