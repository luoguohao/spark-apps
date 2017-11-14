package com.luogh.spark.util

import java.io.{IOException, InputStream, OutputStream}
import java.util.zip.GZIPInputStream

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
  * Created by tend on 2017/6/12.
  */

object HDFSUtil {

  val conf: Configuration = new Configuration
  val fileSystem = try {
    FileSystem.get(conf)
  } catch {
    case e: Exception => throw new IllegalStateException("Init FileSystem failed.", e)
  }
  private val BUFFER_SIZE = 5 * 1024 // 5KB

  @throws[IOException]
  def create(path: String): OutputStream = fileSystem.create(new Path(path), false, BUFFER_SIZE)

  @throws[IOException]
  def open(path: String): InputStream =
    fileSystem.open(new Path(path), fileSystem.getConf.getInt("io.file.buffer.size", BUFFER_SIZE))

  @throws[IOException]
  def openGzip(path: String) = new GZIPInputStream(fileSystem.open(new Path(path),
    fileSystem.getConf.getInt("io.file.buffer.size", BUFFER_SIZE)))

  def exist(path: String) = fileSystem.exists(new Path(path))

}

