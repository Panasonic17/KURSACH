package spark.hive

import model.Traffic
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode
import app.StreamingApp._

class hiveTrafficWriter extends (RDD[Traffic] => Unit)  {
  override def apply(rdd: RDD[Traffic]): Unit = {
    import sparkSess.implicits._

    val result = rdd.toDF("Word", "Count")
    result.coalesce(1)
      .write
      .format("parquet")
      .mode(SaveMode.Append)
      .save("hdfs://sandbox-hdp.hortonworks.com:8020/store/data.parquet")
  }
}
