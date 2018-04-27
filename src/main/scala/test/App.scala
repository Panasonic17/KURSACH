package test

import java.util.Random

import model.Traffic
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.elasticsearch.spark.rdd.EsSpark
import spark.streaming.reciver.SatoriReciver

object App {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("NetworkWordCount").setMaster("local[4]") //.set("es.index.auto.create", "true").set("es.port", "9200").set("es.nodes", "172.17.0.2")
    val ssc = new StreamingContext(sparkConf, Durations.seconds(1))
    ssc.sparkContext.setLogLevel("ERROR")
    val endpoint = "wss://open-data.api.satori.com"
    val appkey = "9fbd1c4BEa889C66cFf83B042B0fDCed"
    val channel = "air-traffic"
    val lines = ssc.receiverStream(new SatoriReciver(endpoint, appkey, channel))
    lines.foreachRDD(rdd => EsSpark.saveToEs(rdd.map(row => Traffic.parce1(row)), "test6/test6"))

    ssc.start()
    try
      ssc.awaitTermination()
    catch {
      case e: InterruptedException =>
        e.printStackTrace()
    }

  }

}
