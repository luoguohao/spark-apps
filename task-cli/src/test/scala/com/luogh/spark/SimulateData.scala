package com.luogh.spark

import java.io.File

import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * @author luogh 
  */
object SimulateData {

  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setAppName(s"${this.getClass.getName}")
    config.setMaster("local[*]")
    val sc = new SparkContext(config)
    val sqlContext = new SQLContext(sc)

    loadParquet(sc, sqlContext)
    readParquet(sc, sqlContext)

    sc.textFile("/tmp/city_combo_times_tdid_cnt_result")
      .filter(!_.isEmpty)
      .map { line =>
        val splits = line.split(",")
        val t = splits.span(liter => {
          try {
            liter.toInt
            false
          } catch {
            case _: Throwable => true
          }
        })
        (s"${t._1.mkString(",")}:${t._2(0)}", t._2(1).toInt)
      }
      .sortBy(_._2, false)
      .map(t => s"${t._1}:${t._2}")
      .saveAsTextFile("/tmp/city_combo_times_tdid_cnt_result_sorted")
  }

  def readParquet(sc: SparkContext, sqlContext: SQLContext): Unit = {
    sqlContext.read.parquet("E:\\fe\\resident_city_cnt").show()
  }


  private def loadParquet(sc: SparkContext, sqlContext: SQLContext) = {
    val records = Source.fromFile(new File("e://fe//resident_city"))
      .getLines().filter(line => line.trim.length > 0)
      .map { line =>
        val splits = line.split(",")
        Row(splits(0), splits(1), splits(2))
      }.toSeq

    val schema = new StructType()
      .add("tdid", StringType, true)
      .add("city", StringType, true)
      .add("times", StringType, true)

    sqlContext.createDataFrame(sc.parallelize(records), schema)
      .write.parquet("/fe/resident_city_cnt")
  }
}
