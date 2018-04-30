package spark.streaming.distance

import model.Coordinates

object Utils {
  def getDistance(lat1: Double, lon1: Double, lat2: Double, lon2: Double): Double = {
    val earthRadius = 6371000 //meters
    val dLat = Math.toRadians(lat2 - lat1)
    val dLng = Math.toRadians(lon2 - lon1)
    val a = Math.sin(dLat / 2) * Math.sin(dLat / 2) + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) * Math.sin(dLng / 2) * Math.sin(dLng / 2)
    val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
    val dist = (earthRadius * c).toFloat
    dist
  }

  def getDistance(lat1: Double, lon1: Double, coordinates: Coordinates): Double = {
    getDistance(lat1: Double, lon1: Double, coordinates.lat: Double, coordinates.lon: Double)
  }
}
