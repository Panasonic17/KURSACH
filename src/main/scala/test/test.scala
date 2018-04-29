package test

import spark.BroadcastGenerator

object test {
  def main(args: Array[String]): Unit = {
    val broadcstTest=new BroadcastGenerator
    println(broadcstTest.getIataCityName("/home/sawa/programing/Kursach/Data/airports.dat"))
    println(broadcstTest.getIataCountryName("/home/sawa/programing/Kursach/Data/airports.dat"))
  }
}
