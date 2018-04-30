package spark.elasticsearch

import org.apache.spark.rdd.RDD
import org.elasticsearch.spark.rdd.EsSpark

case class elkDistance(destination:String,time:Long,flight:String,distance:Double)

object ElasticSearchSender {
  def saveDistancesToElasticsearch(data:RDD[((String, Long,String), Double)])= {
    EsSpark.saveToEs(data.map(row => elkDistance(row._1._1, row._1._2, row._1._3, row._2)), "test_mapping3/test")
  }
}
