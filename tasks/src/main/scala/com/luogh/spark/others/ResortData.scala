package com.luogh.spark.others

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

import scala.collection.mutable

/**
  * @author luogh 
  */
object ResortData {

  def main(args: Array[String]): Unit = {
    require(args.length == 1, "require 1 arguments.")
    val sc = new SparkContext()
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    args(0).toInt match {
      case taskType@(0 | 1 | 3) => {
        val dependencyRDD = sqlContext.sparkContext.textFile("/fe/user/guohao.luo/city_max_times")
          .filter(_.length > 0)
          .map { row =>
            val splits = row.split(",")
            new Record(splits(0), splits(1).toInt, splits(2))
          }
        taskType match {
          case 0 =>
            sameCityCnt(dependencyRDD)
          case 1 =>
            sameCityDistribution(dependencyRDD, sqlContext)
          case 3 =>
            dependencyRDD.toDF.registerTempTable("city_max_table")
            sameCityCntDetail(sqlContext)
        }
      }
      case 2 => refactorResult(sc, sqlContext)
      case _ => sys.error("Invalid taskType")
    }
  }


  def refactorResult(sc: SparkContext, sqlContext: SQLContext): Unit = {
    import sqlContext.implicits._
    val originRdd = sc.textFile("/tmp/city_combo_times_tdid_cnt_result")
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
        (t._1.mkString(","), t._2(0), t._2(1).toInt)
      }
      .sortBy(_._3, false)
      .toDF("cities", "times", "tdid_cnt")
      .write.parquet("/fe/user/guohao.luo/city_combo_times_tdid_cnt_result_refactor")
  }

  // 1. tdid 出现次数最大的城市 频次数据 统计 出现城市相同 tdid个数
  def sameCityCnt(dependencyRDD: RDD[Record]): Unit = {
    val result = dependencyRDD
      .groupBy(r => r.city)
      .filter(_._2.size > 1)
      .flatMapValues(iter => iter.map(_.tdid))
      .map(_._2)
      .groupBy(identity[String] _, 1000) // 按 tdid 分组
      .mapPartitions { iter => Iterator(iter.size) } //分区中TDID个数
      .sum // 总和

    println(s"出现城市相同tdid个数=${result}")
  }

  def sameCityCntDetail(sqlContext: SQLContext): Unit = {
    val sql =
      """
        | select city, times, count(distinct tdid) as cnt
        | from city_max_table group by city, times having cnt > 1
      """.stripMargin

    sqlContext.sql(sql).write.parquet("/fe/user/guohao.luo/city_same_times_tdid_cnt_20171112")
  }

  // 2. tdid  出现次数最大的城市 频次数据 统计 城市组合 出现次数 tdid量
  def sameCityDistribution(dependencyRDD: RDD[Record], sqlContext: SQLContext): Unit = {

    /**
      *
      * 城市  出现次数  tdid
      *
      * 城市组合    组合出现次数    tdid个数
      * (北京:上海)      3              200
      * (北京:天津)      3              200
      *
      * 一个tdid在一年中即出现在北京同时出现在上海并且出现次数相同
      *
      */

    /**
      * 分组
      * 按城市组合 组合出现次数分组
      *
      * @param tuple (tdid,城市组合,cnt)
      * @return
      */
    def grouped(tuple: (String, String, Int)): String = s"${tuple._2};${tuple._3}"

    import sqlContext.implicits._

    val rdd = dependencyRDD.groupBy(_.tdid)
      .flatMap {
        case (tdid, iter) =>
          // tdid 城市组合 cnt
          iter.foldLeft(new mutable.HashMap[Int, mutable.ArrayBuffer[String]]()) { (m, r) =>
            val sets = m.getOrElseUpdate(r.times, new mutable.ArrayBuffer[String]())
            sets += r.city
            m
          }.map {
            case (times, sets) => (tdid, sets.sorted.mkString(","), times)
          }
      }.groupBy(grouped _)
      .mapValues { iter => iter.map(_._1).toSet.size }
      .map {
        case (citiesCnt, tdidCnt) =>
          val splits = citiesCnt.split(";")
          (splits(0), splits(1).toInt, tdidCnt)
      }
      .toDF("cities", "time", "tdid_cnt")
      .write.parquet("/fe/user/guohao.luo/city_combo_times_tdid_cnt_result_20171112")
  }

  case class Record(tdid: String, times: Int, city: String)

}
