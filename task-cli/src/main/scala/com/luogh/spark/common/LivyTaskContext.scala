package com.luogh.spark.common

import java.io.{File, FileFilter, FileNotFoundException}
import java.net.URI

import org.apache.livy.LivyClientBuilder
import org.slf4j.LoggerFactory

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

trait LivyTaskContext extends TaskContext {

  import LivyTaskContext._
  import org.apache.livy.scalaapi._


  val LIVY_SEVER_URL = "http://172.21.58.4:8998"

  var scalaClient: LivyScalaClient = _
  val excludeJarPath = Set("netty-all-4.0.29.Final.jar", "livy-rsc-0.4.0-incubating.jar",
    "livy-api-0.4.0-incubating.jar", "livy-repl_2.10-0.4.0-incubating.jar",
    "commons-codec-1.9.jar", "livy-core_2.10-0.4.0-incubating.jar"
  )

  val sparkAppName: String
  /**
    * Initialize the livy client
    */
  override def init(): this.type = {
    scalaClient = new LivyClientBuilder(true).setURI(new URI(LIVY_SEVER_URL))
      .setConf("spark.app.name", s"TalkingTrip-${sparkAppName}")
      .build()
      .asScalaClient
    this
  }

  override def close(): Unit = {
    if (scalaClient != null) {
      scalaClient.stop(true)
      scalaClient = null
    }
  }

  /**
    * Uploads the Scala-API Jar and the task Jar from the target directory.
    *
    * @throws FileNotFoundException If either of Scala-API Jar or task Jar is not found.
    */
  @throws(classOf[FileNotFoundException])
  def uploadRelevantJarsForJobExecution(): Unit = {
    val appJarPath = getSourcePath(this)
    uploadJar(appJarPath)
    // upload dependency library
    uploadLibraryPath(appJarPath)
  }

  @throws(classOf[FileNotFoundException])
  private def uploadLibraryPath(appJarPath: String): Unit = {
    val file = new File(appJarPath)
    val libDir = new File(file.getParent, "lib")

    val fileNameFilter = new FileFilter {
      override def accept(pathname: File): Boolean = pathname.getName.endsWith("jar") && !excludeJarPath.contains(pathname.getName)
    }

    for (libFile <- libDir.listFiles(fileNameFilter).toSeq.par) {
      uploadJar(libFile.getPath)
    }
  }

  @throws(classOf[FileNotFoundException])
  private def getSourcePath(obj: Any): String = {
    val source = obj.getClass.getProtectionDomain.getCodeSource
    if (source != null && source.getLocation.getPath != "") {
      source.getLocation.getPath
    } else {
      throw new FileNotFoundException(s"Jar containing ${obj.getClass.getName} not found.")
    }
  }

  private def uploadJar(path: String): Unit = {
    val file = new File(path)
    LOGGER.info(s"=====> Trying to upload path ${file.getPath} to Livy Server")
    val uploadJarFuture = scalaClient.uploadJar(file)
    Try(Await.result(uploadJarFuture, Duration.Inf)) match {
      case Success(_) => LOGGER.info("Successfully uploaded " + file.getName)
      case Failure(e) =>
        LOGGER.error(s"upload path ${file.getPath} to Livy Failed.", e)
        throw e
    }
  }
}

object LivyTaskContext {
  val LOGGER = LoggerFactory.getLogger(LivyTaskContext.getClass)
}