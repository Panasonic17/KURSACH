package spark

import model.Coordinates

import scala.io.Source

class BroadcastGenerator {

  def getIataCityName(pathToAiroportsDat: String): Map[String, String] = {
    val lines = Source.fromFile(pathToAiroportsDat).getLines.toList
    lines.map(row => row.split(",")).filter(_.length > 11).map(row => (row(11), row(9))).toMap
  }

  def getIataCountryName(pathToAiroportsDat: String): Map[String, String] = {
    val lines = Source.fromFile(pathToAiroportsDat).getLines.toList
    lines.map(row => row.split(",")).filter(_.length > 11).map(row => (row(11), row(7))).toMap
  }

  def getIataCoorinates(pathToAiroportsDat: String): Map[String, Coordinates] = {
    val lines = Source.fromFile(pathToAiroportsDat).getLines.toList
    lines.map(row => row.split(","))
      .filter(_.length > 11)
      .map(row => {
        try {
          (row(11), Coordinates(row(4).replace("\"", "").toDouble, row(3).replace("\"", "").toDouble))
        } catch {
          case e :Exception => ("",Coordinates(0,0))
        }
      })
      .toMap
  }
}
