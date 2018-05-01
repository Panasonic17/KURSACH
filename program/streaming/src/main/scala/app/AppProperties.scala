package app

import java.io.InputStream
import java.util.Properties


object AppProperties {

  var cloud:Boolean=false

  private def loadProperties: Properties = {
    val _prop = new java.util.Properties()
    val stream: InputStream = this.getClass.getClassLoader.getResourceAsStream("app.properties")
    _prop.load(stream)
    _prop
  }

  private val prop = loadProperties

  lazy val hiveMetastoreUris: String =if (cloud) prop.getProperty("cloud."+"a") else prop.getProperty("home."+"a")

  lazy val pathToData: String =if (cloud) prop.getProperty("cloud."+"dataPath") else prop.getProperty("home."+"dataPath")

}
