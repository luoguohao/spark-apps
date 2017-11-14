package com.luogh.spark.others

import com.luogh.spark.common.TravelSparkParam
import com.luogh.spark.tasks.TrainData
import org.apache.spark.SparkContext

/**
  * @author luogh 
  */
object TrainDataTask {

  def main(args: Array[String]): Unit = {
    assert(args.filterNot(_.isEmpty).length == 3, s"Train Task expect 3 params, current ${args}")
    val tdidPath = args(0).trim
    val trainPath = args(1).trim
    val resultPath = args(2).trim
    val param = new TravelSparkParam(tdidPath, trainPath, resultPath)
    val sc = new SparkContext()
    TrainData.run(param)(sc)
  }
}
