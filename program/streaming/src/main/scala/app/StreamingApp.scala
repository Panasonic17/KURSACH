package app

import model._
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Durations, Seconds, StreamingContext}
import org.elasticsearch.spark.rdd.EsSpark
import spark.BroadcastGenerator
import spark.streaming.distance.DistanceToDestinationCalculator
import spark.streaming.reciver.SatoriReciver

object StreamingApp {

//  val sparkConf = new SparkConf().setAppName("NetworkWordCount").setMaster("local[4]")//.set("es.nodes", "35.230.25.238").set("es.nodes.discovery","false") //.set("es.index.auto.create", "true").set("es.port", "9200").
//  val ssc = new StreamingContext(sparkConf, Durations.seconds(5))
//  val sc = ssc.sparkContext

  val sparkSess = SparkSession.builder().master("local[4]").appName("My App").getOrCreate()
  val sc = sparkSess.sparkContext
  val ssc = new StreamingContext(sc, Seconds(5))


  def main(args: Array[String]): Unit = {

    AppProperties.cloud=if(args.head.toInt==1) true else false

//    val broadcastGenerator = new BroadcastGenerator
//    val cityNames = sc.broadcast(broadcastGenerator.getIataCityName(AppProperties.pathToData))
//    val countryNames = sc.broadcast(broadcastGenerator.getIataCountryName(AppProperties.pathToData))
//
//    val airoportCoordinates: Broadcast[Map[String, Coordinates]] = sc.broadcast(broadcastGenerator.getIataCoorinates(AppProperties.pathToData))

//    ssc.sparkContext.setLogLevel("ERROR")

    val endpoint = "wss://open-data.api.satori.com"
    val appkey = "73eF3D8DF0c428eb7a44e6D14EbAAd9a"
    val channel = "air-traffic"

    var lines:ReceiverInputDStream[String]=null

//    if(AppProperties.cloud)
    lines = ssc.receiverStream(new SatoriReciver(endpoint, appkey, channel))
//    else
//    lines = ssc.socketTextStream("127.0.0.1",1500)

    lines.print(1)
//    val pourTraffic =lines.map(row => Traffic.parceTraffic(row))
//
//    //calculate distance between plain and airoport
//    val plainPositions=pourTraffic.map(traffic=>new PlainPosition(traffic))
//    val distanceCalculator=new DistanceToDestinationCalculator
//    distanceCalculator.calculateDistanceToDestinationAiroports(plainPositions,airoportCoordinates)
//
//    //send to elk
//    val updated: DStream[Traffic] = pourTraffic.map(trafic => trafic.updateCityAndCountry(cityNames.value, countryNames.value))
//    updated.foreachRDD(rdd => EsSpark.saveToEs(rdd, "test_mapping2/te123" ,Map("es.nodes" -> "127.0.0.1","es.nodes.discovery" -> "false","es.nodes.wan.only"->"true")))

    //send to hive

    ssc.start()
    try
      ssc.awaitTermination()
    catch {
      case e: InterruptedException =>
        e.printStackTrace()
    }
  }

}
