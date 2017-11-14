package com.luogh.spark

import com.luogh.spark.others.StatTdidResidenceCity._
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{Test => JuitTest}

/**
  * @author luogh 
  */
class StatTdidResidenceCityTest extends Serializable {

  @JuitTest
  def test(): Unit = {
    val config = new SparkConf().setAppName(s"${this.getClass.getName}")
    config.setMaster("local[*]")
    implicit val sc = new SparkContext(config)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val data = sqlContext.read.parquet("E:\\fe\\resident_city_cnt")
    data.registerTempTable("resident_city")

    println(data.rdd.getNumPartitions)
    //
    //    residentCityResult(sqlContext)
    //
    //    top1ResidentCityCnt(sqlContext)
    //
    //    TdidCntInSameCityCnt(sqlContext)
    //
    //    cityDistributeInSameCnt(data)
    //
    //    cityDistribute(sqlContext)
    //
    //    timeDistributeResult(sqlContext)

    //    cityMaxTimes(data, sqlContext)
    sc.parallelize(List(Person("a", 12, 2), Person("b", 13, 2), Person("a", 12, 2)))
      .toDF.groupBy("name")
      .pivot("age", Seq(12))
      .max("code")
      .show()

    //    cityMaxTimes(data, sqlContext)

    println(s"spark.default.parallelism:{${data.rdd.context.getConf.contains("spark.default.parallelism")}")

    val rdd = data.map { row =>
      val tdid = row.getAs[String]("tdid")
      val city = row.getAs[String]("city")
      val times = row.getAs[String]("times").toInt
      (tdid, times, city)
    }.groupBy(hash _, 100)

    println(s"rdd partition size : ${rdd.partitioner.get.numPartitions}")

    val rr = data.map { row =>
      val tdid = row.getAs[String]("tdid")
      val city = row.getAs[String]("city")
      val times = row.getAs[String]("times").toInt
      (tdid, times, city)
    }.groupBy(_._1)

    println(s"rdd partition size : ${rr.partitioner.get.numPartitions}")

    cityDistributeFlattenInSameCnt(data, createOutputPath("E:\\fe\\user\\guohao.luo\\city_flattern_combo_times_tdid_cnt_result"))
  }

  def hash(x: (String, Int, String)): String = x._1

  @JuitTest
  def test2(): Unit = {
    println(Util.permutation(Seq("tes", "test2", "test3")).map(_.mkString(",")).mkString("  <=> "))
    println(Seq(1 to 10: _*))
  }

  @JuitTest
  def test3(): Unit = {
    val line = "南阳市,宣城市,平顶山市,扬州市,渭南市,焦作市,绍兴市,蚌埠市,2,1"
    val splits = line.split(",")
    println(splits.mkString(" "))
    val t = splits.span(liter => {
      println(liter)
      try {
        liter.toInt
        false
      } catch {
        case e: NumberFormatException => true
      }
    })
    s"${t._1.mkString(",")}:${t._2.mkString(":")}"
  }

  case class Person(name: String, age: Int, code: Int)
}
