package spark

import scala.io.Source

class BroadcastGenerator {

  def getIataCityName(pathToAiroportsDat: String): Any = {
    val lines = Source.fromFile(pathToAiroportsDat).getLines.toList
    lines.map(row => row.split(",")).map(row => (row(4), row(2))).toMap
  }

  def getIataCountryName(pathToAiroportsDat: String): Any = {
    val lines = Source.fromFile(pathToAiroportsDat).getLines.toList
    lines.map(row => row.split(",")).map(row => (row(4), row(3))).toMap
  }

}
