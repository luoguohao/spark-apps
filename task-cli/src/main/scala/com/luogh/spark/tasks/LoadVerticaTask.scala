package com.luogh.spark.tasks

import com.luogh.spark.common._
import com.luogh.spark.util.HDFSUtil

/**
  * @author luogh 
  */
class LoadVerticaTask extends Task {
  type Context = LoadVerticaTaskContext

  override def buildTaskContext(args: Array[String]): Context = {
    assert(args.filterNot(_.isEmpty).length == 2, s"LoadVerticaTask expect 2 params, current ${args}")
    val path = args(0).trim
    assert(HDFSUtil.exist(path), s"InputPath ${path} doesn't exists.")
    val param = LoadVerticaParam(path, args(1).trim)
    new LoadVerticaTaskContext(param)
  }

  override def execute(context: Context): Unit = {
    import VerticaFunction._
    withConnection(this.loadIntoVertica(context.params))(implicitly(connection))
  }
}


class LoadVerticaTaskContext(val params: LoadVerticaParam) extends AbsTaskContext {
  type Param = LoadVerticaParam
}
