package com.luogh.spark.tasks

import com.luogh.spark.common.TravelSparkParam

/**
  * @author luogh 
  */
class TrainTask extends TravelTask {

  override def sparkJob(param: TravelSparkParam) = { jobContext =>
    TrainData.run(param)(jobContext.sc)
  }
}
