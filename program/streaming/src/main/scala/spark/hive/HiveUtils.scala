package spark.hive

import app.AppProperties
import model.{PlainPosition, Traffic}
import org.apache.spark.SparkContext
import org.apache.spark.streaming.dstream.DStream

class HiveUtils {

  def writeToHive(sc:SparkContext, trafficStream :DStream[Traffic]): Unit ={
    while(!AppProperties.cloud){}


  }
}
