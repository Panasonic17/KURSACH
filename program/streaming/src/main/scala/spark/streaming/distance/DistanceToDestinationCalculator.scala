package spark.streaming.distance

import model.{Coordinates, PlainPosition, Traffic}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Duration, Seconds}
import spark.elasticsearch.ElasticSearchSender

class DistanceToDestinationCalculator extends Serializable  {

  def calculateDistanceToDestinationAiroports(traficStream: DStream[PlainPosition], airoportCoordinates: Broadcast[Map[String, Coordinates]]) = {
    val distances=traficStream.window(Seconds(30), Seconds(30)).foreachRDD(rdd => calculateAllDistancesForBatch(rdd, airoportCoordinates))
  }

  def calculateAllDistancesForBatch(plainsPositions: RDD[PlainPosition], airoportCoordinates: Broadcast[Map[String, Coordinates]]): RDD[((String, Long,String), Double)] = {
    val distinctPlains = plainsPositions.map(plainsPositions => (plainsPositions.flight, plainsPositions)).reduceByKey((a, b) => a)
    val disnances = distinctPlains
      .map(plainPosition => (
        (plainPosition._2.destination, plainPosition._2.time,plainPosition._2.flight),
        Utils.getDistance(plainPosition._2.plainLat, plainPosition._2.plainLon, airoportCoordinates.value.getOrElse(plainPosition._2.destination, Coordinates(0, 0)))
      ))
//    disnances.foreach(println)
   ElasticSearchSender.saveDistancesToElasticsearch(disnances)
    disnances
  }

}
