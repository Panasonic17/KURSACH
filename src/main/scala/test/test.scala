package test

import spark.BroadcastGenerator

object test {
  def main(args: Array[String]): Unit = {
//    val s ="1,2,,3,,,,4,5,6"
//    println(s)
//    println(s.split(",").length)
//    println(s.split(",").mkString("|"))
    val broadcstTest=new BroadcastGenerator
    println(broadcstTest.getIataCityName("C:\\WORK_DIR\\Projects\\KURSACH\\Data\\airport-codes.csv"))
    println(broadcstTest.getIataCountryName("C:\\WORK_DIR\\Projects\\KURSACH\\Data\\airport-codes.csv"))
  }
}
