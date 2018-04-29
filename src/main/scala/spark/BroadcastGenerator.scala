package spark

import scala.io.Source

class BroadcastGenerator {

  def getIataCityName(pathToAiroportsDat: String):Map[String,String] = {
    val lines = Source.fromFile(pathToAiroportsDat).getLines.toList
    lines.map(row => row.split(",")).map(row => (row(4), row(2))).toMap
  }

  def getIataCountryName(pathToAiroportsDat: String):Map[String,String] = {
    val lines = Source.fromFile(pathToAiroportsDat).getLines.toList
    lines.map(row => row.split(",")).map(row => (row(4), row(3))).toMap
  }

}
