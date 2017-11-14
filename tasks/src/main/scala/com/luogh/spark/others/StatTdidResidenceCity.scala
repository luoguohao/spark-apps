package com.luogh.spark.others

import com.google.common.base.Objects
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * @author luogh 
  */
object StatTdidResidenceCity {


  def main(args: Array[String]): Unit = {
    assert(args.length == 1, "expect 1 param, indicate the execute type.")

    val config = new SparkConf().setAppName(s"${this.getClass.getName}")
    implicit val sc = new SparkContext(config)
    val sqlContext = new SQLContext(sc)

    val data = sqlContext.read.parquet("/datascience/datacloud/datagroup/data/liuxiaohong/resident_new_flat")
    data.registerTempTable("resident_city")
    //    sqlContext.sql("select count(distinct tdid) from resident_city").show() // 1994850962

    println(s"spark.default.parallelism:${sqlContext.getAllConfs.contains("spark.default.parallelism")}")

    args(0).trim.toInt match {
      case 0 =>
        residentCityResult(sqlContext)
      case 1 =>
        top1ResidentCityCnt(sqlContext)
      case 2 =>
        tdidCntInSameCityCnt(sqlContext)
      case 3 =>
        cityDistributeInSameCnt(data)
      case 4 =>
        cityDistribute(sqlContext)
      case 5 =>
        timeDistributeResult(sqlContext)
      case 6 =>
        cityMaxTimes(data, sqlContext)
      case 7 =>
        cityDistributeInSameCntTest(data, createOutputPath("/fe/user/guohao.luo/test_city_combo_times_tdid_cnt_result"))
      case 8 =>
        cityDistributeFlattenInSameCntError(data, createOutputPath("/fe/user/guohao.luo/city_flattern_combo_times_tdid_cnt_result"))
      case 9 =>
        cityDistributeFlattenInSameCnt(data, createOutputPath("/fe/user/guohao.luo/city_flattern_combo_times_tdid_cnt_result"))
      case _ =>
        sys.error(s"wrong execute type[${args(0)}]")
        sys.exit(1)
    }
  }


  def createOutputPath(outputStr: String)(implicit sc: SparkContext): String = {
    val config = sc.hadoopConfiguration
    val fileSystem = FileSystem.get(config)
    val outPath = new Path(outputStr)
    if (fileSystem.exists(outPath)) {
      println("Exist outputStr path ,delete it.")
      fileSystem.delete(outPath, true)
    }
    outputStr
  }

  def cityDistribute(sqlContext: SQLContext): Unit = {
    // 城市分布情况
    sqlContext.sql("select city, count(distinct tdid) as cnt from resident_city group by city").map { row =>
      val city = row.getAs[String]("city")
      val cnt = row.getAs[Long]("cnt")
      s"${city},${cnt}"
    }.coalesce(50).saveAsTextFile("/fe/user/guohao.luo/city_distribute_stat")
  }

  def cityDistributeInSameCntTest(data: DataFrame, output: String): Unit = {
    val rdd = data.rdd.groupBy(row => s"${row.getAs[String]("tdid")}:${row.getAs[String]("times")}") // 按 tdid times分组
      .map { row =>
      val split = row._1.split(":")
      val tdid = split(0)
      val times = split(1)
      val cities = row._2.map(_.getAs[String]("city"))
      (tdid, times, cities)
    }.filter(row => row._3.size > 1) // 存在一个以上的城市是相同的
      .map { item =>
      (s"${item._2}:${item._3.toSeq.sorted.mkString(",")}", item._1) // cnt:城市列表排序 为key, tdid为value
    }

    println(s"rdd partitions:${rdd.partitions.length}, has partitioner: ${rdd.partitioner.isDefined}")

    val resultRdd = rdd.groupBy(_._1).map { entry => // 根据cnt:城市列表分组，统计对应的tdid数
      val splits = entry._1.split(":")
      val cnt = splits(0)
      val cityList = splits(1)
      val tdidCnt = entry._2.toSeq.map(_._2).distinct.count(_ => true) //计算 tdid个数
      (cityList, cnt, tdidCnt)
    }.mapPartitions { par =>
      par.map(_.productIterator.mkString(","))
    }

    println(s"resultRdd partitions:${resultRdd.partitions.length}, has partitioner: ${resultRdd.partitioner.isDefined}")
    resultRdd.saveAsTextFile(output)
  }

  def cityDistributeFlattenInSameCnt(data: DataFrame, outputPath: String): Unit = {
    /**
      * 原始数据：
      * tdid1 北京  2
      * tdid1 上海  2
      * tdid1 天津  2
      * tdid2 北京  2
      * tdid2 上海  2
      *
      * 城市组合    组合出现次数    tdid个数
      * (北京,上海)      2             1
      * (北京,上海,天津)     2         2
      *
      * 一个tdid在一年中即出现在北京同时出现在上海并且出现次数相同
      *
      */

    def partition(tuple: (String, Int, String)) = s"${tuple._1}:${tuple._2}"

    val rdd = data.map { row =>
      val tdid = row.getAs[String]("tdid")
      val city = row.getAs[String]("city")
      val times = row.getAs[String]("times").toInt
      (tdid, times, city)
    }

    val partiallyOrdered = rdd.groupBy[String](partition _, 700) // 按 tdid times分组
      .map { row =>
      val split = row._1.split(":")
      val tdid = split(0)
      val times = split(1)
      val cities = row._2.map(_._3)
      (tdid, times, cities)
    }.filter(row => row._3.size > 1) // 存在一个以上的城市是相同的
      .map { item =>
      (s"${item._2}:${item._3.toSeq.sorted.mkString(",")}", item._1) // cnt:城市列表排序 为key, tdid为value
    }

    partiallyOrdered.groupBy(_._1).flatMap { entry => // 根据cnt:城市列表分组，统计对应的tdid数
      val splits = entry._1.split(":")
      val cnt = splits(0)
      val tdidCnt = entry._2.map(_._2).toStream.distinct.map(_ => 1).sum //计算 tdid个数
      Util.permutation(splits(1).split(",")).map(city => (s"${city.sorted.mkString(",")}:${cnt}", tdidCnt))
    }.reduceByKey {
      _ + _
    }
      .map { item => s"${item._1}:${item._2}" }
      .saveAsTextFile(outputPath)
  }


  def cityDistributeFlattenInSameCntError(data: DataFrame, outputPath: String): Unit = {
    // 城市出现相同次数的这些tdid的城市分布情况
    /**
      * 原始数据：
      * tdid1 北京  2
      * tdid1 上海  2
      * tdid1 天津  2
      * tdid2 北京  2
      * tdid2 上海  2
      *
      * 城市组合    组合出现次数    tdid个数
      * (北京,上海)      2             1
      * (北京,上海,天津)     2         2
      *
      * 一个tdid在一年中即出现在北京同时出现在上海并且出现次数相同
      *
      */

    def partition(tuple: (String, Int, String)) = s"${tuple._1}:${tuple._2}"

    val rdd = data.map { row =>
      val tdid = row.getAs[String]("tdid")
      val city = row.getAs[String]("city")
      val times = row.getAs[String]("times").toInt
      (tdid, times, city)
    }

    println(s"rdd partitions:${rdd.partitions.length}, has partitioner: ${rdd.partitioner.isDefined}")

    val partiallyOrdered = rdd.groupBy[String](partition _, 700) // 按 tdid times分组
      .map { row =>
      val split = row._1.split(":")
      val tdid = split(0)
      val times = split(1)
      val cities = row._2.map(_._3)
      (tdid, times, cities)
    }.filter(row => row._3.size > 1) // 存在一个以上的城市是相同的
      .map { item =>
      (PartitionKey(item._2, item._3.toSet), PartitionValue(item._1, item._3.toSet))
    }

    // scalastyle:off println
    println(s"partiallyOrdered rdd partitions:${partiallyOrdered.partitions.length}, has partitioner: ${partiallyOrdered.partitioner.isDefined}")
    // scalastyle:on println

    partiallyOrdered.groupByKey(700).flatMap { entry => // 根据PartitionKey分组，统计对应的tdid数
      val partitionKey = entry._1
      val cnt = partitionKey.cnt
      // 找出分组中的城市列表的公共交集,并计算他的tdid个数
      val intersectValue = entry._2.foldLeft(IntersectValue(citySet = partitionKey.citySet)) { (interValue, item) =>
        IntersectValue(interValue.tdidCnt + 1, interValue.citySet.intersect(item.citySet))
      }
      // 计算非交集的所有城市列表
      val buff = entry._2.filter(value => value.citySet.union(intersectValue.citySet).size > intersectValue.citySet.size)
        .groupBy(v => v.citySet.toIndexedSeq.sorted.mkString(","))
        .mapValues(iter => iter.toStream.map(v => v.tdid).distinct.size)
        .map(kv => s"${kv._1},${cnt},${kv._2}")
        .toBuffer
      buff += s"${intersectValue.citySet.toIndexedSeq.sorted.mkString(",")},${cnt},${intersectValue.tdidCnt}"
      buff
    }.coalesce(10).saveAsTextFile(outputPath)
  }

  def cityDistributeInSameCnt(data: DataFrame): Unit = {
    // 城市出现相同次数的这些tdid的城市分布情况
    /**
      * 城市组合    组合出现次数    tdid个数
      * (北京,上海)      3              200
      * (北京,天津)      3              200
      *
      * 一个tdid在一年中即出现在北京同时出现在上海并且出现次数相同
      *
      */

    def partition(tuple: (String, Int, String)) = s"${tuple._1}:${tuple._2}"

    val rdd = data.map { row =>
      val tdid = row.getAs[String]("tdid")
      val city = row.getAs[String]("city")
      val times = row.getAs[String]("times").toInt
      (tdid, times, city)
    }

    println(s"rdd partitions:${rdd.partitions.length}, has partitioner: ${rdd.partitioner.isDefined}")

    val partiallyOrdered = rdd.groupBy[String](partition _, 700) // 按 tdid times分组
      .map { row =>
      val split = row._1.split(":")
      val tdid = split(0)
      val times = split(1)
      val cities = row._2.map(_._3)
      (tdid, times, cities)
    }.filter(row => row._3.size > 1) // 存在一个以上的城市是相同的
      .map { item =>
      (s"${item._2}:${item._3.toSeq.sorted.mkString(",")}", item._1) // cnt:城市列表排序 为key, tdid为value
    }

    println(s"partiallyOrdered rdd partitions:${partiallyOrdered.partitions.length}, has partitioner: ${partiallyOrdered.partitioner.isDefined}")

    partiallyOrdered.groupBy(_._1).map { entry => // 根据cnt:城市列表分组，统计对应的tdid数
      val splits = entry._1.split(":")
      val cnt = splits(0)
      val cityList = splits(1)
      val tdidCnt = entry._2.map(_._2).toStream.distinct.map(_ => 1).sum //计算 tdid个数
      s"${cityList};${cnt};${tdidCnt}"
    }.saveAsTextFile("/fe/user/guohao.luo/city_combo_times_tdid_cnt_result")
  }

  def tdidCntInSameCityCnt(sqlContext: SQLContext): Unit = {
    // 城市出现相同次数的tdid量
    sqlContext.sql("select count(distinct tdid) as cnt from (select tdid, times, count(distinct city) as t from resident_city " +
      "group by tdid, times having t > 1 ) as tmp_a")
      .map(row => {
        row.getAs[Long]("cnt")
      }).coalesce(50).saveAsTextFile("/fe/user/guohao.luo/resident_city_same_times_cnt_result")
  }

  def top1ResidentCityCnt(sqlContext: SQLContext): Unit = {
    //城市(top1) -> tdid个数
    sqlContext.sql("select city, count(distinct tdid) as tdid_cnt from " +
      "(select tdid, city, max(times) from resident_city group by tdid, city) as tmp_a group by city " +
      " order by tdid_cnt desc").map(row => {
      val city = row.getAs[String]("city")
      val tdidCnt = row.getAs[Long]("tdid_cnt")
      s"${city},${tdidCnt}"
    }).coalesce(50).saveAsTextFile("/fe/user/guohao.luo/resident_city_tdid_cnt_result")
  }

  def residentCityResult(sqlContext: SQLContext): Unit = {
    // 出现城市个数 -> tdid个数
    sqlContext.sql("select cnt, count(distinct tdid) as tdid_cnt" +
      " from ( select tdid, count(distinct city) as cnt from resident_city group by tdid ) as tmp " +
      "group by cnt").rdd.map(row => {
      val cnt = row.getAs[Long]("cnt")
      val tdidCnt = row.getAs[Long]("tdid_cnt")
      s"${cnt},${tdidCnt}"
    }).coalesce(50).saveAsTextFile("/fe/user/guohao.luo/tdid_resident_city_result")
  }

  def timeDistributeResult(sqlContext: SQLContext): Unit = {
    // 出现次数 -> tdid个数
    sqlContext.sql("select times, count(distinct tdid) as cnt from resident_city group by times order by cnt desc").rdd.map(row => {
      val times = row.getAs[String]("times")
      val cnt = row.getAs[Long]("cnt")
      s"${times},${cnt}"
    }).coalesce(50).saveAsTextFile("/fe/user/guohao.luo/time_distribute_result")
  }

  // tdid 出现次数最大的城市 频次
  def cityMaxTimes(data: DataFrame, sqlContext: SQLContext): Unit = {
    def partition(tuple: (String, Int, String)) = tuple._1

    data.map { row =>
      val tdid = row.getAs[String]("tdid")
      val city = row.getAs[String]("city")
      val times = row.getAs[String]("times").toInt
      (tdid, times, city)
    }.groupBy(partition _, 700)
      .mapValues { row =>
        val iter = row.toSeq.sortBy(_._2).reverseIterator
        val buff = new ArrayBuffer[(String, Int, String)]
        var maxTime = -1
        var completed = false
        while (iter.hasNext && !completed) {
          val next = iter.next()
          if (buff.size == 0) {
            buff += next
            maxTime = next._2
          } else {
            if (next._2 != maxTime) {
              completed = true
            }
          }
        }
        buff
      }
      .flatMap { item =>
        item._2.map { r => s"${r._1},${r._2},${r._3}" }
      }.saveAsTextFile("/fe/user/guohao.luo/city_max_times")
  }

  // 1. tdid 出现次数最大的城市 频次数据 统计 出现城市相同 tdid个数
  def sameCityCnt(sqlContext: SQLContext): Unit = {
    sqlContext.sparkContext.textFile("/fe/user/guohao.luo/city_max_times")
      .map { row => }
    // 城市出现相同次数的tdid量
    sqlContext.sql("select count(distinct tdid) as cnt from (select tdid, times, count(distinct city) as t from resident_city " +
      "group by tdid, times having t > 1 ) as tmp_a")
      .map(row => {
        row.getAs[Long]("cnt")
      }).coalesce(50).saveAsTextFile("/fe/user/guohao.luo/resident_city_same_times_cnt_result")
  }

  case class PartitionValue(tdid: String = "", citySet: Set[String] = Set())

  case class PartitionKey(cnt: String, citySet: Set[String]) extends Ordered[PartitionKey] {

    override def equals(that: Any) = {
      that match {
        case e: PartitionKey =>
          // 当cnt值相同，并且城市列表共同包含的城市个数大于0，则认为是相同的partitionKey
          if (this.cnt.equals(e.cnt)) {
            if (this.citySet.intersect(e.citySet).size > 0) true else false
          } else {
            false
          }
        case _ => false
      }
    }


    override def hashCode(): Int = {
      Objects.hashCode(this.cnt)
    }

    override def compare(that: PartitionKey) = {
      if (this.cnt >= that.cnt) {
        if (this.citySet.size >= that.citySet.size) {
          1
        } else {
          -1
        }
      } else {
        -1
      }
    }
  }

  case class IntersectValue(tdidCnt: Int = 0, citySet: Set[String] = Set())

  // 2. tdid  出现次数最大的城市 频次数据 统计 城市组合 出现次数 tdid量

  object Util {
    /**
      * 排列组合
      * 1,2,3 三个数据的排列组合为:
      * 1;2;3;1,2;1,3;2,3;1,2,3 总共七种
      *
      * @param list
      * @return
      */
    def permutation[T](list: Seq[T]) = {
      val result = new ArrayBuffer[Seq[Int]]
      for (i <- 1 to list.size) {
        if (i == 1) {
          (0 until list.size).foreach(i => result += Seq(i))
        } else {
          for (item <- result if item.length == i - 1) {
            for (j <- i - 1 until list.size) {
              // 大于最大值
              if (item.max < j) {
                result += (item.toBuffer += j)
              }
            }
          }

        }
      }
      val zip = list.zipWithIndex.map(x => (x._2, x._1)).toMap
      result.map(_.map(item => zip.get(item).get))
    }
  }

}
