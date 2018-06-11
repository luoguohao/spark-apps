package com.luogh.spark.others

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author luogh 
  */
object ReadPar {

  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setAppName(s"${this.getClass.getName}")
    val sc = new SparkContext(config)
    val sqlContext = new SQLContext(sc)
    val datas = sqlContext.read.load(s"C:/Users/luogh/Desktop/27/*.gz.parquet")
    datas.printSchema()
  }

}
