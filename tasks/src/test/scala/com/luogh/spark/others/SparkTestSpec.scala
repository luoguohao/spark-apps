package com.luogh.spark.others

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FunSpec

/**
  * @author luogh 
  */
class SparkTestSpec extends FunSpec {

  private val conf = new SparkConf().setMaster("local[*]").setAppName("TEST")

  private val sc = new SparkContext(conf)
  private val sqlContext = new SQLContext(sc)


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
}
