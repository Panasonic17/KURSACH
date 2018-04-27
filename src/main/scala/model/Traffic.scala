package model

import org.json4s.{DefaultFormats, _}
import org.json4s.native.JsonMethods._

case class Traffic(origin: String, flight: String, course: Int, aircraft: String, callsign: String, registration: String, lat: Double, speed: Int, altitude: String, destination: String, lon: Double, time: Long) {

}

object Traffic {
  def parceTraffic(json: String): Traffic = {
    val parsed = parse(json)
    implicit val formats = DefaultFormats
    val traffic = parsed.extract[Traffic]
    Traffic(traffic.origin, traffic.flight, traffic.course, traffic.aircraft, traffic.callsign, traffic.registration, traffic.lat, traffic.speed, traffic.altitude, traffic.destination, traffic.lon, traffic.time * 1000L)
  }

  def main(args: Array[String]): Unit = {
    val test = "{\n  \"origin\": \"ARN\",\n  \"flight\": \"SK637\",\n  \"course\": 216,\n  \"aircraft\": \"A20N\",\n  \"callsign\": \"SAS61Z\",\n  \"registration\": \"SE-ROB\",\n  \"lat\": 57.1295,\n  \"speed\": 437,\n  \"altitude\": 39025,\n  \"destination\": \"FRA\",\n  \"lon\": 14.4414,\n  \"time\": 1524748817\n}"
    print(parceTraffic(test))
  }
}


