package model

case class PlainPosition(flight:String,plainLon:Double,plainLat:Double,destination:String,time:Long) {
  def this(traffic: Traffic) = this(traffic.flight,traffic.lon,traffic.lat,traffic.destination,traffic.time)
}
