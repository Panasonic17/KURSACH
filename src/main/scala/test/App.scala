package test

import java.util.Random

import model.{Coordinates, PlainPosition, Traffic}
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.elasticsearch.spark.rdd.EsSpark
import spark.BroadcastGenerator
import spark.streaming.distance.DistanceToDestinationCalculator
import spark.streaming.reciver.SatoriReciver

object App {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("NetworkWordCount").setMaster("local[4]")//.set("es.nodes", "35.230.25.238").set("es.nodes.discovery","false") //.set("es.index.auto.create", "true").set("es.port", "9200").
    val ssc = new StreamingContext(sparkConf, Durations.seconds(5))
    val sc = ssc.sparkContext


    val broadcastGenerator = new BroadcastGenerator
    val cityNames = sc.broadcast(broadcastGenerator.getIataCityName("C:\\WORK_DIR\\Projects\\KURSACH\\Data\\airport-codes.csv"))
    val countryNames = sc.broadcast(broadcastGenerator.getIataCountryName("C:\\WORK_DIR\\Projects\\KURSACH\\Data\\airport-codes.csv"))

    val airoportCoordinates: Broadcast[Map[String, Coordinates]] = sc.broadcast(broadcastGenerator.getIataCoorinates("C:\\WORK_DIR\\Projects\\KURSACH\\Data\\airport-codes.csv"))

    ssc.sparkContext.setLogLevel("ERROR")

    val endpoint = "wss://open-data.api.satori.com"
    val appkey = "9fbd1c4BEa889C66cFf83B042B0fDCed"
    val channel = "air-traffic"

    val lines = ssc.receiverStream(new SatoriReciver(endpoint, appkey, channel))
    val pourTraffic =lines.map(row => Traffic.parceTraffic(row))

    //calculate distance between plain and airoport
    val plainPositions=pourTraffic.map(traffic=>new PlainPosition(traffic))
    val distanceCalculator=new DistanceToDestinationCalculator
//    distanceCalculator.calculateDistanceToDestinationAiroports(plainPositions,airoportCoordinates)

    val updated: DStream[Traffic] = pourTraffic.map(trafic => trafic.updateCityAndCountry(cityNames.value, countryNames.value))
    updated.foreachRDD(rdd => EsSpark.saveToEs(rdd, "test_mapping2/te123" ,Map("es.nodes" -> "35.230.25.238","es.nodes.discovery" -> "false","es.nodes.wan.only"->"true")))
    ssc.start()
    try
      ssc.awaitTermination()
    catch {
      case e: InterruptedException =>
        e.printStackTrace()
    }

  }

}
