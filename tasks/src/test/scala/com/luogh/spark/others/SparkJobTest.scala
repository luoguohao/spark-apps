package com.luogh.spark.others

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author luogh 
  */
object SparkJobTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("test")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val data = sc.textFile("/data/test_city_tdid.txt")
      .map { line =>
        val split = line.split(",")
        CityTdid(split(0), split(1), split(2))
      }.toDF
    StatTdidResidenceCity.cityMaxTimes(data, sqlContext)
  }

  case class CityTdid(tdid: String, times: String, city: String)
}
