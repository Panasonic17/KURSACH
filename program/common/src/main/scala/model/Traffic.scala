package model

import org.json4s.native.JsonMethods._
import org.json4s.{DefaultFormats, _}


case class Traffic(origin: String, flight: String, course: Int, aircraft: String, callsign: String, registration: String, lat: Double, speed: Int, altitude: String, destination: String, lon: Double, time: Long, var originCity: String, var destinationCity: String, var originCountry: String, var destinationCountry: String) {
  def this(origin: String, flight: String, course: Int, aircraft: String, callsign: String, registration: String, lat: Double, speed: Int, altitude: String, destination: String, lon: Double, time: Long) = this(origin: String, flight: String, course: Int, aircraft: String, callsign: String, registration: String, lat: Double, speed: Int, altitude: String, destination: String, lon: Double, time: Long, "", "", "", "")

  def updateCityAndCountry(cities: Map[String, String], countries: Map[String, String]): Traffic = {
    originCity=cities.getOrElse(origin,"")
    destinationCity=cities.getOrElse(destination, "")
    originCountry=countries.getOrElse(origin,"")
    destinationCountry=countries.getOrElse(destination, "")
    this
  }

}

object Traffic {
  def parceTraffic(json: String): Traffic = {
    val parsed = parse(json)
    implicit val formats = DefaultFormats
    val traffic = parsed.extract[Traffic]
    Traffic(traffic.origin, traffic.flight, traffic.course, traffic.aircraft, traffic.callsign, traffic.registration, traffic.lat, traffic.speed, traffic.altitude, traffic.destination, traffic.lon, traffic.time * 1000L, "", "", "", "")
  }

  def main(args: Array[String]): Unit = {
//    val broadcstTest=new BroadcastGenerator
//   val m1= broadcstTest.getIataCityName("/home/sawa/programing/Kursach/Data/airports.dat")
//   val m2= broadcstTest.getIataCountryName("/home/sawa/programing/Kursach/Data/airports.dat")
//    val test = "{\n  \"origin\": \"ARN\",\n  \"flight\": \"SK637\",\n  \"course\": 216,\n  \"aircraft\": \"A20N\",\n  \"callsign\": \"SAS61Z\",\n  \"registration\": \"SE-ROB\",\n  \"lat\": 57.1295,\n  \"speed\": 437,\n  \"altitude\": 39025,\n  \"destination\": \"FRA\",\n  \"lon\": 14.4414,\n  \"time\": 1524748817\n}"
//    val traffic = parceTraffic(test).updateCityAndCountry(m1,m2)
//    print(traffic)
  }
}


