package proj.common

import java.sql.Date

case class FlightData(passengerId: Integer, flightId: Integer, from: String, to: String, date: Date)
