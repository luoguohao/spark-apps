package com.luogh.spark.others

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FunSpec}

/**
  * @author luogh 
  */
class SparkTestSpec extends FunSpec with BeforeAndAfter { suit =>

  val master = "local"
  val appName = "example-spark"
  var sc: SparkContext = _

  before {
    val conf = new SparkConf().setMaster(master).setAppName(appName)
    sc = new SparkContext(conf)
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }


  describe("测试combineByKey|reduceBykey|groupByKey算子") {
    it("测试combineByKey") {
      var rdd1 = sc.makeRDD(Array(("A", 1), ("A", 2), ("B", 1), ("B", 2), ("B", 3), ("B", 4), ("C", 1)), 7)
      rdd1.combineByKey(
        (v: Int) => v + "_",
        (c: String, v: Int) => c + "@" + v,
        (c1: String, c2: String) => c1 + "$" + c2
      ).collect.foreach(println)
    }
  }

  describe("测试spark分区统计") {
    it("分区总和应该为3") {
      val seq = Seq("1", "2", "2", "2", "3")
      val result = sc.parallelize(seq)
        .groupBy(identity[String] _, 10)
        .mapPartitions { iter =>
          Seq(iter.toSeq.size).iterator
        } //分区统计
        .sum // 总和

      assert(result === 3)
    }
  }

  describe("计算一年中每个tdid出现次数相同的城市组合统计") {

    it("城市组合 -> 出现次数 -> tdid个数") {
      val sqlContext = new SQLContext(sc)
      import sqlContext.implicits._

      val data = sc.textFile("/data/test_city_tdid.txt")
        .map { line =>
          val split = line.split(",")
          CityTdid(split(0), split(1), split(2))
        }.toDF

      StatTdidResidenceCity.cityMaxTimes(data, sqlContext)
    }
  }
}

case class CityTdid(tdid: String, times: String, city: String)
