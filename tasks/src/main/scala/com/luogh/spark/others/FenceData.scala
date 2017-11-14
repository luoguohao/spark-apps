package com.luogh.spark.others

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author luogh 
  */
object FenceData {

  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setAppName(s"${this.getClass.getName}")
    val sc = new SparkContext(config)
    val sqlContext = new SQLContext(sc)

    // 高德围栏 2017-04 3599
    val gdFence = sc.textFile("/fe/atm/msp-job-service/201711/061822122141679594294/output_detail_device/result.gz")

    // 手工围栏 2017-04 1015
    val handFence = sc.textFile("/fe/atm/msp-job-service/201711/0618244650334433017/output_detail_device/result.gz")

    // 交集 1015
    val joinData = gdFence.map((_, 1)).distinct.join(handFence.map((_, 1)).distinct).count()


    // 高德围栏 2017-06 4726
    val gdFence06 = sc.textFile("/fe/atm/msp-job-service/201711/06185133582142871183/output_detail_device/result.gz")

    // 手工围栏 2017-06 1092
    val handFence06 = sc.textFile("/fe/atm/msp-job-service/201711/061852340141913385286/output_detail_device/result.gz")

    // 交集 1092
    val joinData06 = gdFence06.map((_, 1)).distinct.join(handFence06.map((_, 1)).distinct).count()


  }

}
