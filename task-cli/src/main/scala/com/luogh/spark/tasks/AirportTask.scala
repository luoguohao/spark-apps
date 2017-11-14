package com.luogh.spark.tasks

import com.luogh.spark.common.TravelSparkParam

/**
  * @author luogh 
  */
class AirportTask extends TravelTask {

  override def sparkJob(param: TravelSparkParam) = { jobContext =>
    AirportData.run(param)(jobContext.sc)
  }
}
